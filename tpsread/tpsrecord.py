from construct import Byte, Bytes, Embedded, EmbeddedSwitch, Enum, IfThenElse, Peek, PaddedString, Struct, Switch, Int32ub, Int16ul, Int32ul, Probe, Const, this

from .tpspage import PAGE_HEADER_STRUCT
from .utils import check_value

record_encoding = 'ascii'

#RECORD_TYPE = 'type' / Enum(Byte,
#                   NULL=None,
#                   DATA=0xF3,
#                   METADATA=0xF6,
#                   TABLE_DEFINITION=0xFA,
#                   TABLE_NAME=0xFE,
#                   _default_='INDEX', )
RECORD_TYPE = 'type' / Enum(Byte,
                   NULL=None,
                   DATA=0xF3,
                   METADATA=0xF6,
                   TABLE_DEFINITION=0xFA,
                   TABLE_NAME=0xFE,)

DATA_RECORD_DATA = 'field_data' / Struct('record_number' / Int32ub,
                                         'data' / Bytes(this.data_size - 9))

METADATA_RECORD_DATA = 'field_metadata' / Struct('metadata_type' / Byte,
                              'metadata_record_count' / Int32ul,
                              'metadata_record_last_access' / Int32ul)

TABLE_DEFINITION_RECORD_DATA = 'table_definition' / Struct('table_definition_bytes' / Bytes(this.data_size - 5))

INDEX_RECORD_DATA = 'field_index' / Struct(
                           'data_i' / Bytes(this.data_size - 10),
                           'record_number_i' / Int32ul)

#print(INDEX_RECORD_DATA)

RECORD_STRUCT =   'record' / EmbeddedSwitch( Struct('data_size' / Int16ul,
                                              'first_byte' / Peek(Byte),),
#                         False,
                        this.first_byte == 0xFE, # 'record_type' / IfThenElse,
                                        {True: Struct(
                                                     'type_n' / RECORD_TYPE,
                                                     'table_name' / PaddedString(this.data_size - 5,
                                                            encoding=record_encoding),
                                                     'table_number_n' / Int32ub,
                                                     ),
                                         False: EmbeddedSwitch(Struct(
                                                     'table_number' / Int32ub,
                                                     RECORD_TYPE,),
                                                     this.type,
                                                      {
                                                       'DATA': (DATA_RECORD_DATA),
                                                       'METADATA': (METADATA_RECORD_DATA),
                                                       'TABLE_DEFINITION': (
                                                                TABLE_DEFINITION_RECORD_DATA),
                                                       'INDEX': (INDEX_RECORD_DATA)
                                                       })} )


class TpsRecord:
    def __init__(self, header_size, data):
        self.header_size = header_size
        self.data_bytes = data
        # print(data)

        data_size = len(self.data_bytes) - 2

        # print('data_size', data_size, header_size)

        if data_size == 0:
            self.type = 'NULL'
        else:
            #print("self.data_bytes=", self.data_bytes)
            self.data = RECORD_STRUCT.parse(self.data_bytes)
            
            # Small workaround for EmbeddedSwitch inability to share a field name
            if self.data.table_number_n is not None:
                self.data.table_number = self.data.table_number_n
            if self.data.type_n is not None:
                self.data.type = self.data.type_n
             
            #TODO: Index records are ignored...
                
            #print("self.data=", self.data)
            self.type = self.data.type
            #print('record type:', self.data.type)


class TpsRecordsList:
    def __init__(self, tps, tps_page, encoding=None, check=False):
        self.tps = tps
        self.check = check
        self.tps_page = tps_page
        self.encoding = encoding
        global record_encoding
        record_encoding = encoding
        self.__records = []

        if self.tps_page.hierarchy_level == 0:
            if self.tps_page.ref in self.tps.cache_pages:
                self.__records = tps.cache_pages[self.tps_page.ref]
            else:
                data = self.tps.read(self.tps_page.size - PAGE_HEADER_STRUCT.sizeof(),
                                     self.tps_page.ref * 0x100 + self.tps.header.size + PAGE_HEADER_STRUCT.sizeof())

                if self.tps_page.uncompressed_size > self.tps_page.size:
                    data = self.__uncompress(data)

                    if self.check:
                        check_value('record_data.size', len(data) + PAGE_HEADER_STRUCT.sizeof(),
                                    tps_page.uncompressed_size)

                record_data = b''
                pos = 0
                record_size = 0
                record_header_size = 0

                while pos < len(data):
                    byte_counter = data[pos]
                    pos += 1
                    if (byte_counter & 0x80) == 0x80:
                        record_size = data[pos + 1] * 0x100 + data[pos]
                        pos += 2
                    if (byte_counter & 0x40) == 0x40:
                        record_header_size = data[pos + 1] * 0x100 + data[pos]
                        pos += 2
                    byte_counter &= 0x3F
                    new_data_size = record_size - byte_counter
                    record_data = record_data[:byte_counter] + data[pos:pos + new_data_size]
                    self.__records.append(TpsRecord(record_header_size, ('data_size' / Int16ul).build(record_size)
                                                    + record_data))
                    pos += new_data_size

                if self.tps.cached and self.tps_page.ref not in tps.cache_pages:
                    tps.cache_pages[self.tps_page.ref] = self.__records

    def __uncompress(self, data):
        pos = 0
        result = b''
        while pos < len(data):
            repeat_rel_offset = data[pos]
            pos += 1

            if repeat_rel_offset > 0x7F:
                # size repeat_count = 2 bytes
                repeat_rel_offset = ((data[pos] << 8) + ((repeat_rel_offset & 0x7F) << 1)) >> 1
                pos += 1

            result += data[pos:pos + repeat_rel_offset]
            pos += repeat_rel_offset

            if pos < len(data):
                repeat_byte = bytes(result[-1:])
                repeat_count = data[pos]
                pos += 1

                if repeat_count > 0x7F:
                    repeat_count = ((data[pos] << 8) + ((repeat_count & 0x7F) << 1)) >> 1
                    pos += 1

                result += repeat_byte * repeat_count
        return result

    def __getitem__(self, key):
        return self.__records[key]
