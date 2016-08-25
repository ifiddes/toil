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

AWSUserData="""#cloud-config

write_files:
    - path: "/home/core/volumes.sh"
      permissions: "0777"
      owner: "root"
      content: |
        #!/bin/bash
        set -x
        ephemeral_count=0
        possible_drives="/dev/xvdb /dev/xvdc /dev/xvdd /dev/xvde"
        drives=""
        directories="toil mesos docker"
        for drive in $possible_drives; do
            echo checking for $drive
            if [ -b $drive ]; then
                echo found it
                ephemeral_count=$((ephemeral_count + 1 ))
                drives="$drives $drive"
                echo increased ephemeral count by one
            fi
        done
        if (("$ephemeral_count" == "0" )); then
            echo no ephemeral drive
            for directory in $directories; do
                sudo mkdir -p /var/lib/$directory
            done
            exit 0
        fi
        sudo mkdir /mnt/ephemeral
        for drive in $drives; do
            dd if=/dev/zero of=$drive bs=4096 count=1024
        done
        if (("$ephemeral_count" == "1" )); then
            echo one ephemeral drive to mount
            sudo mkfs.ext4 -F $drives
            sudo mount $drives /mnt/ephemeral
        fi
        if (("$ephemeral_count" > "1" )); then
            echo multiple drives
            sudo mdadm --create -f --verbose /dev/md0 --level=0 --raid-devices=$ephemeral_count $drives # determine force flag
            sudo mkfs.ext4 -F /dev/md0
            sudo mount /dev/md0 /mnt/ephemeral
        fi
        for directory in $directories; do
            sudo mkdir -p /mnt/ephemeral/var/lib/$directory
            sudo mkdir -p /var/lib/$directory
            sudo mount --bind /mnt/ephemeral/var/lib/$directory /var/lib/$directory
        done

coreos:
    update:
      reboot-strategy: off
    units:
    - name: "volume-mounting.service"
      command: "start"
      content: |
        [Unit]
        Description=mounts ephemeral volumes & bind mounts toil directories
        Author=cketchum@ucsc.edu
        After=docker.service

        [Service]
        Restart=always
        ExecStart=/usr/bin/bash /home/core/volumes.sh

    - name: "toil-{role}.service"
      command: "start"
      content: |
        [Unit]
        Description=toil-{role} container
        Author=cketchum@ucsc.edu
        After=docker.service

        [Service]
        Restart=always
        ExecStart=/usr/bin/docker run --net=host -v /var/lib/mesos:/var/lib/mesos -v /var/lib/docker:/var/lib/docker -v /var/lib/toil:/var/lib/toil --name={role} cket/toil-{role}:{tag} {args}

"""
