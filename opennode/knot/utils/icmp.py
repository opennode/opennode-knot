import socket
import subprocess

from ping import do_one
from twisted.python import log


def ping(dest_addr, timeout=2, count=2, psize=64):
    """Pings a given host and returns True if it answers, otherwise False.
    It tries to use a pure python icmp library which requires begin root,
    so there is a `fping` fallback which is useful when running as a developer

    """

    try:
        plist = []
        for i in xrange(count):
            try:
                delay = do_one(dest_addr, timeout, psize)
            except socket.gaierror, e:
                log.msg("ping %s failed. (socket error: '%s')" % (dest_addr, e[1]), system='ping')
                break

            if delay is not None:
                delay = delay * 1000
                plist.append(delay)

        # Find lost package percent
        lost = 100 - (len(plist) * 100 / count)
        return lost != 100
    except:
        try:
            return not subprocess.call('fping -q %s' % dest_addr, shell=True)
        except:
            return True
