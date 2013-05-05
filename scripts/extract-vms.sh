#!/bin/bash

ssh $1 ls -l /machines/$2/vms/ | awk '{print $3, $2}' | sed 's/.\[[0-9]*;[0-9]*m\(.*\)\//\1/g' | grep '[[:alnum:]]*-[[:alnum:]]*-[[:alnum:]]*-[[:alnum:]]*-[[:alnum:]]*' --color=no
