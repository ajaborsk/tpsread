"""
TPS File Table
"""

from construct import Array, BitsInteger, BitStruct, Byte, Const, CString, Embedded, Enum, Flag, If, Padding, Struct, Int16ul, Probe, this, len_

from .tpsrecord import TpsRecordsList


FIELD_TYPE_STRUCT = 'type' / Enum(Byte,
                         BYTE=0x1,
                         SHORT=0x2,
                         USHORT=0x3,
                         # date format 0xYYYYMMDD
                         DATE=0x4,
                         # time format 0xHHMMSSHS
                         TIME=0x5,
                         LONG=0x6,
                         ULONG=0x7,
                         FLOAT=0x8,
                         DOUBLE=0x9,
                         DECIMAL=0x0A,
                         STRING=0x12,
                         CSTRING=0x13,
                         PSTRING=0x14,
                         # compound data structure
                         GROUP=0x16,
                         # LIKE (inherited data type)
)

TABLE_DEFINITION_FIELD_STRUCT = 'record_table_definition_field' / Struct(
                                       FIELD_TYPE_STRUCT,
                                       # data offset in record
                                       'offset' / Int16ul,
                                       'name' / CString('ascii'),
                                       'array_element_count' / Int16ul,
                                       'size' / Int16ul,
                                       # 1, if fields overlap (OVER attribute), or 0
                                       'overlaps' / Int16ul,
                                       # record number
                                       'number' / Int16ul,
                                       'array_element_size' / If(this.type == 'STRING' or this.type == 'CSTRING' or this.type == 'PSTRING' or this.type == 'PICTURE', Int16ul),
                                       'template' / If(this.type == 'STRING' or this.type == 'CSTRING' or this.type == 'PSTRING' or this.type == 'PICTURE', Int16ul),
#                                       'dummy' / If(this.type == 'CSTRING', Embedded('array_element_size' / Int16ul)),
#                                       'dummy' / If(this.type == 'CSTRING', Embedded('template' / Int16ul)),
#                                       'dummy' / If(this.type == 'PSTRING', Embedded('array_element_size' / Int16ul)),
#                                       'dummy' / If(this.type == 'PSTRING', Embedded('template' / Int16ul)),
#                                       'dummy' / If(this.type == 'PICTURE', Embedded('array_element_size' / Int16ul)),
#                                       'dummy' / If(this.type == 'PICTURE', Embedded('template' / Int16ul)),
                                       'decimal_count' / If(this.type == 'DECIMAL', Byte),
                                       'decimal_size' / If(this.type == 'DECIMAL', Byte),
                                       )
                                       # Original code has this kwarg... allow_overwrite=True, 

INDEX_TYPE_STRUCT = 'type' / Enum(BitsInteger(2),
                         KEY=0,
                         INDEX=1,
                         DYNAMIC_INDEX=2)

INDEX_FIELD_ORDER_TYPE_STRUCT = 'field_order_type' / Enum(Int16ul,
                                     ASCENDING=0,
                                     DESCENDING=1,
                                     _default_='DESCENDING')

TABLE_DEFINITION_INDEX_STRUCT = 'record_table_definition_index' / Struct(
                                       # May be external_filename
                                       # if external_filename == 0, no external file index
                                       'external_filename' / CString('ascii'),
                                       If(len_(this.external_filename) == 0, 'index_mark' / Const(1, Byte)),
                                       'name' / CString('ascii'),
#                                       Embedded('flags' / BitStruct(
#                                                       Padding(1),
#                                                       INDEX_TYPE_STRUCT,
#                                                       Padding(2),
#                                                       'NOCASE' / Flag,
#                                                       'OPT' / Flag,
#                                                       'DUP' / Flag)),
                                       'flags' / BitStruct(
                                                       Padding(1),
                                                       INDEX_TYPE_STRUCT,
                                                       Padding(2),
                                                       'NOCASE' / Flag,
                                                       'OPT' / Flag,
                                                       'DUP' / Flag),
                                       'field_count' / Int16ul,
                                       Array(this.field_count,
                                             'index_field_propertly' / Struct(
                                                    'field_number' / Int16ul,
                                                    INDEX_FIELD_ORDER_TYPE_STRUCT)), )

MEMO_TYPE_STRUCT = 'memo_type' / Enum(Flag,
                        MEMO=0,
                        BLOB=1)

TABLE_DEFINITION_MEMO_STRUCT = 'record_table_definition_memo' / Struct(
                                      # May be external_filename
                                      # if external_filename == 0, no external file index
                                      'external_filename' / CString('ascii'),
                                      If(len_(this.external_filename) == 0, 'memo_mark' / Const(0, Byte)), # Original had a const '1' but it seems a '0' is required...
                                      'name' / CString('ascii'),
                                      'size' / Int16ul,
#                                      Embedded('flags' / BitStruct(
#                                                      Padding(5),
#                                                      MEMO_TYPE_STRUCT,
#                                                      'BINARY' / Flag,
#                                                      'Flag' / Flag,
#                                                      Padding(8))), 
                                      'flags' / BitStruct(
                                                      Padding(5),
                                                      MEMO_TYPE_STRUCT,
                                                      'BINARY' / Flag,
                                                      'Flag' / Flag,
                                                      Padding(8)), )

TABLE_DEFINITION_STRUCT = 'record_table_definition' / Struct(
                                 'min_version_driver' / Int16ul,
                                 # sum all fields sizes in record
                                 'record_size' / Int16ul,
                                 'field_count' / Int16ul,
                                 'memo_count' / Int16ul,
                                 'index_count' / Int16ul,
                                 'record_table_definition_field' / Array(this.field_count, TABLE_DEFINITION_FIELD_STRUCT),
                                 'record_table_definition_memo' / Array(this.memo_count, TABLE_DEFINITION_MEMO_STRUCT),
                                 'record_table_definition_index' / Array(this.index_count, TABLE_DEFINITION_INDEX_STRUCT), )


class TpsTable:
    def __init__(self, number):
        self.number = number
        self.name = ''
        self.definition_bytes = {}
        self.definition = ''
        self.statistics = {}

    @property
    def iscomplete(self):
        # TODO check all parts complete
        if self.name != '':
            self.get_definition()
            return True
        else:
            return False

    def add_definition(self, definition):
        portion_number = ('portion_number' / Int16ul).parse(definition[:2])
        #print("portion_number = ", portion_number, definition)
        self.definition_bytes[portion_number] = definition[2:]
        #print(self, self.definition_bytes)

    def add_statistics(self, statistics_struct):
        # TODO remove metadatatype from staticstics_struct
        self.statistics[statistics_struct.metadata_type] = statistics_struct

    def get_definition(self):
        definition_bytes = b''
        for value in self.definition_bytes.values():
            definition_bytes += value
        #print(self, "definition_bytes:", definition_bytes)
        self.definition = TABLE_DEFINITION_STRUCT.parse(definition_bytes)
        return self.definition

    def set_name(self, name):
        self.name = name


class TpsTablesList:
    def __init__(self, tps, encoding=None, check=False):
        self.__tps = tps
        self.encoding = encoding
        self.check = check
        self.__tables = {}

        # get tables definition
        i = 0
        d = None
        s = None
        for page_ref in reversed(self.__tps.pages.list()):
            if self.__tps.pages[page_ref].hierarchy_level == 0:
                for record in TpsRecordsList(self.__tps, self.__tps.pages[page_ref],
                                             encoding=self.encoding, check=self.check):
                    i += 1
                    if record.type != 'NULL' and record.data.table_number not in self.__tables.keys():
                        self.__tables[record.data.table_number] = TpsTable(record.data.table_number)
                    if record.type == 'TABLE_NAME':
                        print('Table name set...')
                        print('  to:', record.data.table_name)
                        self.__tables[record.data.table_number].set_name(record.data.table_name)
                    if record.type == 'TABLE_DEFINITION':
                        print('Table definition read...')
                        self.__tables[record.data.table_number].add_definition(record.data.table_definition_bytes)
                        #d = i
                    if record.type == 'METADATA':
                        #print('Table metadata read...')
                        #print(record.data)
                        self.__tables[record.data.table_number].add_statistics(record.data)
                        #s = i
                    #TODO optimize (table_definition and metadata(statistics))
                    if self.__iscomplete():
                        break
                if self.__iscomplete():
                    break
                    #print('stats:', i, d, s, len(self.__tps.pages.list()))
                    #TODO raise exception: No definition found

    def __iscomplete(self):
        for i in self.__tables:
            if not self.__tables[i].iscomplete:
                return False
        if len(self.__tables) == 0:
            return False
        else:
            return True

    def get_definition(self, number):
        return self.__tables[number].get_definition()

    def get_number(self, name):
        for i in self.__tables:
            print("table ", i, "name=", self.__tables[i].name)
            if self.__tables[i].name == name:
                return i
