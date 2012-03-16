#!/bin/env python
""" Utility for stress polling OpenNode agent for hn and vm metrics.  """

import sys
import time

import func.overlord.client as fc

sleep_time = sys.argv[1]
nr_of_iterations = sys.argv[2]
hosts = sys.argv[3:]

if len(sys.argv) < 3:
    print "Usage: %s sleep_in_s nr_of_iterations host1 host2 host3" % sys.argv[0]
    sys.exit(1)

client = fc.Client(";".join(hosts))

for i in range(int(nr_of_iterations)):
    print "====== Iteration : %s of %s ======" % (i, nr_of_iterations)
    print client.onode.metrics()
    print client.onode.vm.metrics('openvz:///system')
    time.sleep(float(sleep_time))

