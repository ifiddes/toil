# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import subprocess
import time
import logging
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from boto.exception import BotoServerError
from cgcloud.lib.ec2 import ec2_instance_types, retry_ec2
from itertools import islice

from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.cgcloud.provisioner import CGCloudProvisioner
from toil.provisioners.aws import *
from cgcloud.lib.context import Context
from boto.utils import get_instance_metadata


logger = logging.getLogger(__name__)


def expectedLaunchErrors(e):
    return e.status == 400 and ('iamInstanceProfile.arn is invalid' in e.body or 'has no associated IAM' in e.body)


def expectedShutdownErrors(e):
    return e.status == 400 and 'dependent object' in e.body


class AWSProvisioner(AbstractProvisioner):

    def __init__(self, config, batchSystem):
        self.ctx = Context(availability_zone='us-west-2a', namespace='/')
        self.instanceMetaData = get_instance_metadata()
        self.securityGroupName = self.instanceMetaData['security-groups']
        self.spotBid = None
        parsedBid = config.nodeType.split(':', 1)
        if len(config.nodeType) != len(parsedBid[0]):
            # there is a bid
            self.spotBid = parsedBid[1]
            self.instanceType = ec2_instance_types[parsedBid[0]]
        else:
            self.instanceType = ec2_instance_types[config.nodeType]
        self.batchSystem = batchSystem
        self.masterIP = self.instanceMetaData['local-ipv4']
        self.keyName = self.instanceMetaData['public-keys'].keys()[0]

    def setNodeCount(self, numNodes, preemptable=False, force=False):
        # get all nodes in cluster
        workerInstances = self._getWorkersInCluster(preemptable)
        instancesToLaunch = numNodes - len(workerInstances)
        logger.info('Adjusting cluster size by %s', instancesToLaunch)
        if instancesToLaunch > 0:
            self._addNodes(instancesToLaunch, preemptable=preemptable)
        elif instancesToLaunch < 0:
            self._removeNodes(instances=workerInstances, numNodes=numNodes, preemptable=preemptable, force=force)
        else:
            pass
        workerInstances = self._getWorkersInCluster(preemptable)
        return len(workerInstances)

    def getNodeShape(self, preemptable=False):
        instanceType = self.instanceType
        return Shape(wallTime=60 * 60,
                     memory=instanceType.memory * 2 ** 30,
                     cores=instanceType.cores,
                     disk=(instanceType.disks * instanceType.disk_capacity * 2 ** 30))

    @staticmethod
    def sshCluster(securityGroupName):
        ctx = Context(availability_zone='us-west-2a', namespace='/')
        instances = AWSProvisioner.__getNodesInCluster(ctx, securityGroupName, both=True)
        key = lambda x: x.launch_time
        instances.sort(key=key)
        master = instances[0]  # assume master was launched first
        masterIP = master.ip_address
        command = "ssh -o \"StrictHostKeyChecking=no\" -t core@%s \"docker exec -it leader bash\"" % masterIP
        subprocess.check_call(command, shell=True)

    @staticmethod
    def launchCluster(instanceType, keyName, securityGroupName, spotBid=None):
        ctx = Context(availability_zone='us-west-2a', namespace='/')
        profileARN = AWSProvisioner._getProfileARN(ctx, instanceID='leader', role='leader')
        AWSProvisioner._createSecurityGroup(ctx, securityGroupName)
        leaderData = {'role': 'leader', 'tag': leaderTag, 'args': leaderArgs}
        userData = AWSUserData.format(**leaderData)
        if not spotBid:
            for attempt in retry_ec2(retry_while=expectedLaunchErrors):
                with attempt:
                    ctx.ec2.run_instances(image_id=coreOSAMI, security_groups=[securityGroupName],
                                          instance_type=instanceType,
                                          instance_profile_arn=profileARN, key_name=keyName,
                                          user_data=userData)
        else:
            requests = []
            for attempt in retry_ec2(retry_while=expectedLaunchErrors):
                with attempt:
                    requests = ctx.ec2.request_spot_instances(price=spotBid, image_id=coreOSAMI,
                                                              count=1, key_name=keyName,
                                                              security_groups=[securityGroupName],
                                                              instance_type=instanceType,
                                                              instance_profile_arn=profileARN,
                                                              user_data=userData,
                                                              launch_group=securityGroupName)
            assert requests
            AWSProvisioner._waitOnRequests(ctx, requests)

    @staticmethod
    def destroyCluster(securityGroupName):
        ctx = Context(availability_zone='us-west-2a', namespace='/')
        instances = AWSProvisioner.__getNodesInCluster(ctx, securityGroupName, both=True)
        spotIDs = AWSProvisioner._getSpotRequestIDs(ctx, securityGroupName)
        if spotIDs:
            ctx.ec2.cancel_spot_instance_requests(request_ids=spotIDs)
        if instances:
            AWSProvisioner._deleteIAMProfiles(instances=instances, ctx=ctx)
            AWSProvisioner._terminateInstance(instances=instances, ctx=ctx)
        logger.info('Deleting security group...')
        for attempt in retry_ec2(retry_after=30, retry_for=300, retry_while=expectedShutdownErrors):
            with attempt:
                ctx.ec2.delete_security_group(name=securityGroupName)
        logger.info('... Succesfully deleted security group')

    @staticmethod
    def _terminateInstance(instances, ctx):
        instanceIDs = [x.id for x in instances]
        logger.info('Terminating instance(s): %s', instanceIDs)
        ctx.ec2.terminate_instances(instance_ids=instanceIDs)
        logger.info('Instance(s) terminated.')

    @staticmethod
    def _deleteIAMProfiles(instances, ctx):
        instanceProfiles = [x.instance_profile for x in instances]
        for profile in instanceProfiles:
            profile_name = profile['arn'].split('/', 1)[1]
            try:
                ctx.iam.remove_role_from_instance_profile(profile_name, 'toil-appliance-worker')
            except BotoServerError as e:
                if e.status == 404:
                    ctx.iam.remove_role_from_instance_profile(profile_name, 'toil-appliance-leader')
                else:
                    raise
            ctx.iam.delete_instance_profile(profile_name)

    def _addNodes(self, instancesToLaunch, preemptable=None):
        bdm = self._getBlockDeviceMapping()
        profileID = str(time.time())
        arn = self._getProfileARN(self.ctx, instanceID=profileID, role='worker')
        workerData = {'role': 'worker', 'tag': workerTag, 'args': workerArgs.format(self.masterIP)}
        userData = AWSUserData.format(**workerData)
        if not preemptable:
            logger.debug('Launching non-preemptable instance(s)')
            for attempt in retry_ec2(retry_while=expectedLaunchErrors):
                with attempt:
                    self.ctx.ec2.run_instances(image_id=coreOSAMI, min_count=instancesToLaunch,
                                               max_count=instancesToLaunch, key_name=self.keyName,
                                               security_groups=[self.securityGroupName],
                                               instance_type=self.instanceType.name,
                                               instance_profile_arn=arn, user_data=userData,
                                               block_device_map=bdm)
        else:
            logger.debug('Launching spot instance(s) with bid of %s', self.spotBid)
            requests = []
            for attempt in retry_ec2(retry_while=expectedLaunchErrors):
                with attempt:
                    # returns list of SpotInstanceRequests
                    requests = self.ctx.ec2.request_spot_instances(price=self.spotBid, image_id=coreOSAMI,
                                                                   count=instancesToLaunch, key_name=self.keyName,
                                                                   security_groups=[self.securityGroupName],
                                                                   instance_type=self.instanceType.name,
                                                                   instance_profile_arn=arn, user_data=userData,
                                                                   block_device_map=bdm)
            self._waitOnRequests(self.ctx, requests)

        logger.info('Launched %s new instance(s)', instancesToLaunch)

    def _getBlockDeviceMapping(self):
        # determine number of ephemeral drives via cgcloud-lib
        bdtKeys = ['', '/dev/xvdb', '/dev/xvdc', '/dev/xvdd']
        bdm = BlockDeviceMapping()
        # the first disk is already attached for us so start with 2nd.
        for disk in xrange(1, self.instanceType.disks + 1):
            bdm[bdtKeys[disk]] = BlockDeviceType(
                ephemeral_name='ephemeral{}'.format(disk - 1))  # ephemeral counts start at 0

        logger.debug('Device mapping: %s', bdm)
        return bdm

    @staticmethod
    def _waitOnRequests(ctx, requests):
        fulfilled = False
        ids = [x.id for x in requests]
        while not fulfilled:
            # wait here until the spot requests are filled and the instances are at least pending
            # this simplifies the rest of the code by limiting the spot related code to this branch
            requests = ctx.ec2.get_all_spot_instance_requests(request_ids=ids)
            ids = [x.id for x in requests if x.status.code != 'fulfilled']
            if len(ids) == 0:
                fulfilled = True
            else:
                logger.info(
                    'Spot requests with ids: %s are not yet fulfilled. Checking again in 15 sec..', ids)
                time.sleep(15)

    def _removeNodes(self, instances, numNodes, preemptable=False, force=False):
        # most of this code is taken directly from toil.provisioners.cgcloud.provisioner.CGCloudProvisioner._removeNodes()
        logger.debug('Attempting to delete nodes')
        # If the batch system is scalable, we can use the number of currently running workers on
        # each node as the primary criterion to select which nodes to terminate.
        if isinstance(self.batchSystem, AbstractScalableBatchSystem):
            logger.debug('Using a scalable batch system')
            nodes = self.batchSystem.getNodes(preemptable)
            # Join nodes and instances on private IP address.
            nodes = [(instance, nodes.get(instance.private_ip_address)) for instance in instances]
            # Unless forced, exclude nodes with runnning workers. Note that it is possible for
            # the batch system to report stale nodes for which the corresponding instance was
            # terminated already. There can also be instances that the batch system doesn't have
            # nodes for yet. We'll ignore those, too, unless forced.
            nodes = [(instance, nodeInfo)
                     for instance, nodeInfo in nodes
                     if force or nodeInfo is not None and nodeInfo.workers < 1]
            # Sort nodes by number of workers and time left in billing cycle
            nodes.sort(key=lambda (instance, nodeInfo): (
                nodeInfo.workers if nodeInfo else 1,
                CGCloudProvisioner._remainingBillingInterval(instance)))
            nodes = nodes[numNodes:]
            instancesTerminate = [instance for instance, nodeInfo in nodes]
        else:
            # Without load info all we can do is sort instances by time left in billing cycle.
            instances = sorted(instances,
                               key=lambda instance: (CGCloudProvisioner._remainingBillingInterval(instance)))
            instancesTerminate = [instance for instance in islice(instances, numNodes)]
        if instancesTerminate:
            self._deleteIAMProfiles(instances=instancesTerminate, ctx=self.ctx)
            self._terminateInstance(instances=instancesTerminate, ctx=self.ctx)
        else:
            logger.debug('No nodes to delete')
        return len(instancesTerminate)

    @staticmethod
    def __getNodesInCluster(ctx, securityGroupName, preemptable=False, both=False):
        pendingInstances = ctx.ec2.get_only_instances(filters={'instance.group-name': securityGroupName,
                                                               'instance-state-name': 'pending'})
        runningInstances = ctx.ec2.get_only_instances(filters={'instance.group-name': securityGroupName,
                                                               'instance-state-name': 'running'})
        instances = set(pendingInstances)
        if not preemptable and not both:
            return [x for x in instances.union(set(runningInstances)) if x.spot_instance_request_id is None]
        elif preemptable and not both:
            return [x for x in instances.union(set(runningInstances)) if x.spot_instance_request_id is not None]
        elif both:
            return [x for x in instances.union(set(runningInstances))]

    def _getNodesInCluster(self, preeptable=False, both=False):
        if not both:
            return self.__getNodesInCluster(self.ctx, self.securityGroupName, preemptable=preeptable)
        else:
            return self.__getNodesInCluster(self.ctx, self.securityGroupName, both=both)

    def _getWorkersInCluster(self, preemptable):
        entireCluster = self._getNodesInCluster(both=True)
        logger.debug('All nodes in cluster %s', entireCluster)
        workerInstances = [i for i in entireCluster if i.private_ip_address != self.masterIP and
                           preemptable != (i.spot_instance_request_id is None)]
        logger.debug('Workers found in cluster after filtering %s', workerInstances)
        return workerInstances

    @staticmethod
    def _getSpotRequestIDs(ctx, securityGroupName):
        requests = ctx.ec2.get_all_spot_instance_requests()
        return [x.id for x in requests if x.launch_group == securityGroupName]

    @staticmethod
    def _createSecurityGroup(ctx, name):
        # security group create/get. ssh + all ports open within the group
        web = ctx.ec2.create_security_group(name, 'Toil appliance security group')
        # open port 22 for ssh-ing
        web.authorize(ip_protocol='tcp', from_port=22, to_port=22, cidr_ip='0.0.0.0/0')
        # the following authorizes all port access within the web security group
        web.authorize(ip_protocol='tcp', from_port=0, to_port=9000, src_group=web)
        return name

    @staticmethod
    def _getProfileARN(ctx, instanceID, role):
        roleName = 'toil-appliance-' + role
        awsInstanceProfileName = roleName
        policy = dict(iam_full=iam_full_policy, ec2_full=ec2_full_policy,
                      s3_full=s3_full_policy, sbd_full=sdb_full_policy)
        ctx.setup_iam_ec2_role(role_name=roleName, policies=policy)

        try:
            profile = ctx.iam.get_instance_profile(awsInstanceProfileName)
        except BotoServerError as e:
            if e.status == 404:
                profile = ctx.iam.create_instance_profile(awsInstanceProfileName)
                profile = profile.create_instance_profile_response.create_instance_profile_result
            else:
                raise
        else:
            profile = profile.get_instance_profile_response.get_instance_profile_result
        profile = profile.instance_profile
        profile_arn = profile.arn

        if len(profile.roles) > 1:
                raise RuntimeError('Did not expect profile to contain more than one role')
        elif len(profile.roles) == 1:
            # this should be profile.roles[0].role_name
            if profile.roles.member.role_name == roleName:
                return profile_arn
            else:
                ctx.iam.remove_role_from_instance_profile(awsInstanceProfileName,
                                                          profile.roles.member.role_name)
        ctx.iam.add_role_to_instance_profile(awsInstanceProfileName, roleName)
        return profile_arn
