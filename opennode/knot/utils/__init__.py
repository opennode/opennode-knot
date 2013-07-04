import struct
from os import urandom

def mac_addr_kvm_generator():
    return 'DE:AD:BE:EF:%02X:%02X' % struct.unpack('BB', urandom(2))
