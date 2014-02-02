#!/bin/bash

# Script for migrating username-based authentication to UID-based.
# Supports changes of UID and re-chowning of VMs.

VMLIST_LOCATION=/tmp/vms.txt
PASSWD_FILE=/etc/opennode/oms_passwd
TMP_PASSWD_FILE=/etc/opennode/oms_passwd.u2i
UID_TMP_FILE=/tmp/u2i.mapping.tmp
OMS_ADMIN_USER=opennode

function uservm {
	grep "\ $1\ " $VMLIST_LOCATION |awk '{print $4 }'|sed -r "s:\x1B\[[0-9;]*[mK]::g" | tr '\@' ' ' | \
	xargs -i echo ssh -p 6022 opennode@localhost "chown -R $2 /computes/"{}
}

# prepare a new oms_passwd
grep -v None $PASSWD_FILE | awk -F  ":" '{print $4,$2,$3,$4}' |tr ' ' ':'  > $TMP_PASSWD_FILE

# create a mapping script
grep -v None $PASSWD_FILE | awk -F  ":" '{print $1, $4}' |tr ' ' ':'  > $UID_TMP_FILE

#Get all VMs
ssh -p 6022 $OMS_ADMIN_USER@localhost ls -l /computes > $VMLIST_LOCATION

# get a list of VMs of a certain user (with old username), generate an ssh comand for chowining to a new one
for i in `cat $UID_TMP_FILE`; do
	old=$(echo $i | cut -f1 -d:)
	uid=$(echo $i | cut -f2 -d:)
	echo -e "\t Changing ownership $old => $uid"
	uservm $old $uid
done

# replace old passwd file with a new one
#mv new_oms_passwd oms_passwd
