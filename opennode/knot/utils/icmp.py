import subprocess

from ping import quiet_ping


def ping(address):
    """Pings a given host and returns True if it answers, otherwise False.
    It tries to use a pure python icmp library which requires begin root,
    so there is a `fping` fallback which is useful when running as a developer

    """

    try:
        lost, _, _ = quiet_ping(address, count=2)
        return lost != 100
    except:
        try:
            return not subprocess.call('fping -q %s' % address, shell=True)
        except:
            return True
