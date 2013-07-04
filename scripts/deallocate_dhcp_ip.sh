#!/bin/bash
# Copyright (C) 2013, OpenNode LLC. All rights reserved.
# Email: info@opennodecloud.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
#
# DESCRIPTION:
# Remove MAC-IP mapping from a DHCP server

# sanity check
if [ $# -ne 6 ]
then
    echo "Incorrect number of arguments."
    echo "Usage: `basename $0` DHCP_KEY DHCP_SERVER DHCP_SERVER_PORT VM_MAC VM_IP VM_NAME"
    exit 1
fi

#DHCP SERVER CONFIGURATION
DHCP_KEY="$1"  # secret DHCP server key
DHCP_SERVER=$2  # ip addres of a DHCP OMAPI server
DHCP_SERVER_PORT=$3  # port where DHCP OMAPI server is listening (default=7911)

VM_MAC=$4
VM_IP=$5
VM_NAME=$6

echo "Updating $DHCP_SERVER:$DHCP_SERVER_PORT with $VM_MAC=$VM_IP ($VM_NAME)".

cat <<EOF |omshell
port $DHCP_SERVER_PORT
key omapi_key "$DHCP_KEY"
server $DHCP_SERVER
connect
new "host"
set name="$VM_NAME"
set hardware-address=$VM_MAC
set hardware-type=1
set ip-address = $VM_IP
open
remove
EOF

