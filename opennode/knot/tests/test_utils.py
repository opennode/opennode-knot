import unittest
from opennode.knot.utils import mac_addr_kvm_generator


class UtilsTest(unittest.TestCase):

    def test_mac_addr_kvm_generator(self):
        mac = mac_addr_kvm_generator()
        assert len(mac) == 17
        assert ':' in mac
        assert mac.startswith('52:54:00')
