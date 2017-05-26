# -*- coding: utf8 -*-

import struct
from ots2.error import *

class PlainBufferInputStream(object):
    def __init__(self, data_buffer):
        self.buffer = data_buffer
        self.cur_pos = 0
        self.last_tag = 0

    def is_at_end(self): 
        return len(self.buffer) == self.cur_pos

    def read_tag(self):
        if self.is_at_end():
            self.last_tag = 0
            return 0

        self.last_tag = self.read_raw_byte()
        return ord(self.last_tag)

    def check_last_tag_was(self, tag):
        return ord(self.last_tag) == tag

    def get_last_tag(self):
        return ord(self.last_tag)

    def read_raw_byte(self):
        if self.is_at_end():
            raise OTSClientError("Read raw byte encountered EOF.")

        pos = self.cur_pos
        self.cur_pos += 1
        return bytes(self.buffer[pos])

    def read_raw_little_endian64(self):
        b1 = ord(self.read_raw_byte())
        b2 = ord(self.read_raw_byte())
        b3 = ord(self.read_raw_byte())
        b4 = ord(self.read_raw_byte())
        b5 = ord(self.read_raw_byte())
        b6 = ord(self.read_raw_byte())
        b7 = ord(self.read_raw_byte())
        b8 = ord(self.read_raw_byte())
        return ((long(b1) & 0xff)) | ((long(b2) & 0xff) << 8) | ((long(b3) & 0xff) << 16) | \
            ((long(b4) & 0xff) << 24) | ((long(b5) & 0xff) << 32) | ((long(b6) & 0xff) << 40) |\
            ((long(b7) & 0xff) << 48) | ((long(b8) & 0xff) << 56)

    def read_raw_little_endian32(self):
        b1 = ord(self.read_raw_byte())
        b2 = ord(self.read_raw_byte())
        b3 = ord(self.read_raw_byte())
        b4 = ord(self.read_raw_byte())
        return ((int(b1) & 0xff)) | ((int(b2) & 0xff) << 8) | ((int(b3) & 0xff) << 16) | ((int(b4) & 0xff) << 24)
    
    def read_boolean(self):
        return self.read_raw_byte() != 0

    def read_double(self):
        return self.read_raw_little_endian64()

    def read_int32(self):
        return self.read_raw_little_endian32()

    def read_int64(self):
        return self.read_raw_little_endian64()

    def read_bytes(self, size):
        if len(self.buffer) - self.cur_pos < size:
            raise OTSClientError("Read bytes encountered EOF.")

        tmp_pos = self.cur_pos
        self.cur_pos += size
        return self.buffer[tmp_pos: tmp_pos + size]

    def read_utf_string(self, size):
        if len(self.buffer) - self.cur_pos < size:
            raise OTSClientError("Read UTF string encountered EOF.")
        utf_str = self.buffer[self.cur_pos:self.cur_pos + size]
        self.cur_pos += size
        return utf_str

        
class PlainBufferOutputStream(object):
    def __init__(self, capacity):
        self.buffer = bytearray()
        self.capacity = capacity

    def get_buffer(self):
        return self.buffer

    def is_full(self):
        return self.capacity<= len(self.buffer)

    def count(self):
        return len(self.buffer)

    def remain(self):
        return self.capacity - self.count()

    def clear(self):
        self.buffer = bytearray('')

    def write_raw_byte(self, value):
        if self.is_full():
            raise OTSClientError("The buffer is full")
        self.buffer.append(value)

    def write_raw_little_endian32(self, value):
        self.write_raw_byte((value) & 0xFF)
        self.write_raw_byte((value >> 8) & 0xFF)
        self.write_raw_byte((value >> 16) & 0xFF)
        self.write_raw_byte((value >> 24) & 0xFF)

    def write_raw_little_endian64(self, value):
        self.write_raw_byte(int(value) & 0xFF)
        self.write_raw_byte(int(value >> 8) & 0xFF)
        self.write_raw_byte(int(value >> 16) & 0xFF)
        self.write_raw_byte(int(value >> 24) & 0xFF)
        self.write_raw_byte(int(value >> 32) & 0xFF)
        self.write_raw_byte(int(value >> 40) & 0xFF)
        self.write_raw_byte(int(value >> 48) & 0xFF)
        self.write_raw_byte(int(value >> 56) & 0xFF)
        
    def write_double(self, value):
        #ba = bytearray(struct.pack("f", value)) 
        ba, = struct.unpack('l', struct.pack('d', value))
        self.write_raw_little_endian64(ba)

    def write_boolean(self, value):
        if value:
            self.write_raw_byte(1)
        else:
            self.write_raw_byte(0)

    def write_bytes(self, value):
        if len(self.buffer) + len(value) > self.capacity:
            raise OTSClientError("The buffer is full.")
        self.buffer += bytearray(value)
