import struct
from os import urandom

def mac_addr_kvm_generator():
    bytelist = struct.unpack('BBB', urandom(3))
    bytelist[0] = bytelist[0] % 0x7f
    return '52:54:00:%02X:%02X:%02X' % (bytelist)
