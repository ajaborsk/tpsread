"""
Class to read TPS files

http://www.clarionlife.net/content/view/41/29/
http://www.softvelocity.com/clarion/pdf/languagereferencemanual.pdf
http://www.softvelocity.com/clarion/pdf/databasedrivers.pdf
"""

import os.path
import mmap
from datetime import date
import time
from warnings import warn
from binascii import hexlify

from six import text_type
from construct import Array, Byte, Bytes, Const, Float32l, Float64l, Struct, Int16sl, Int32sl, Int32ub, \
    Int8ul, Int16ul, Int32ul, ConstError

from .tpscrypt import TpsDecryptor
from .tpstable import TpsTablesList
from .tpspage import TpsPagesList
from .tpsrecord import TpsRecordsList
from .utils import check_value




# Date structure
DATE_STRUCT = 'date_struct' / Struct('day' / Byte,
                     'month' / Byte,
                     'year' / Int16ul, ) 

# Time structure
TIME_STRUCT = 'time_struct' / Struct(
                     'centisecond' / Byte,
                     'second' / Byte,
                     'minute' / Byte,
                     'hour' / Byte)


class TPS:
    """
    TPS file
    """

    def __init__(self, filename, encoding=None, password=None, cached=True, check=False,
                 current_tablename=None, date_fieldname=None,
                 time_fieldname=None, decryptor_class=TpsDecryptor):
        self.filename = filename
        self.encoding = encoding
        self.password = password
        self.cached = cached
        self.check = check
        self.current_table_number = None
        # Name part before .tps
        self.name = os.path.basename(filename)
        self.name = text_type(os.path.splitext(self.name)[0]).lower()
        if date_fieldname is not None:
            self.date_fieldname = date_fieldname
        else:
            self.date_fieldname = []
        if time_fieldname is not None:
            self.time_fieldname = time_fieldname
        else:
            self.time_fieldname = []
        self.cache_pages = {}

        if not os.path.isfile(self.filename):
            raise FileNotFoundError(self.filename)

        self.file_size = os.path.getsize(self.filename)

        # Check file size
        if check:
            if self.file_size & 0x3F != 0:
                # TODO check translate
                warn('File size is not a multiple of 64 bytes.', RuntimeWarning)

        with open(self.filename, mode='r+b') as tpsfile:
            self.tps_file = mmap.mmap(tpsfile.fileno(), 0)

            self.decryptor = decryptor_class(self.tps_file, self.password)

            try:
                # TPS file header
                header = 'header' / Struct(
                                'offset' / Int32ul,
                                'size' / Int16ul,
                                'file_size' / Int32ul,
                                'allocated_file_size' / Int32ul,
                                'top_speed_mark' / Const(b'tOpS\x00\x00', Bytes(6)),
                                'last_issued_row' / Int32ub,
                                'change_count' / Int32ul,
                                'page_root_ref' / Int32ul,
                                'block_start_ref' / Array(lambda ctx: (ctx['size'] - 0x20) // 2 // 4, Int32ul),
                                'block_end_ref' / Array(lambda ctx: (ctx['size'] - 0x20) // 2 // 4, Int32ul))
                print("Reading header...")
                self.header = header.parse(self.read(0x200))
                print("Reading pages...")
                self.pages = TpsPagesList(self, self.header.page_root_ref, check=self.check)
                print("Reading tables...")
                self.tables = TpsTablesList(self, encoding=self.encoding, check=self.check)
                self.set_current_table(current_tablename)
            except ConstError as errr:
                print('Bad cryptographic keys.', self.header, errr)

    def block_contains(self, start_ref, end_ref):
        for i in range(len(self.header.block_start_ref)):
            if self.header.block_start_ref[i] <= start_ref and end_ref <= self.header.block_end_ref[i]:
                return True
        return False

    def read(self, size, pos=None):
        if pos is not None:
            self.seek(pos)
        else:
            pos = self.tps_file.tell()
        if self.decryptor.is_encrypted():
            return self.decryptor.decrypt(size, pos)
        else:
            return self.tps_file.read(size)

    def seek(self, pos):
        self.tps_file.seek(pos)

    def __iter__(self):


        table_definition = self.tables.get_definition(self.current_table_number)
        #print("n, table_definition", self.current_table_number, table_definition)
        for page_ref in self.pages.list():
            if self.pages[page_ref].hierarchy_level == 0:
                for record in TpsRecordsList(self, self.pages[page_ref], encoding=self.encoding, check=self.check):
                    if record.type == 'DATA' and record.data.table_number == self.current_table_number:
                        #print(record, record.data, record.data.data)
                        check_value('table_record_size', len(record.data.data), table_definition.record_size)
                        # TODO convert name to string
                        fields = {"b':RecNo'": record.data.record_number}
                        for field in table_definition.record_table_definition_field:
                            field_data = record.data.data[field.offset:field.offset + field.size]
                            value = ''
                            if field.type == 'BYTE':
                                value = ('byte' / Int8ul).parse(field_data)
                            elif field.type == 'SHORT':
                                value = ('short' / Int16sl).parse(field_data)
                            elif field.type == 'USHORT':
                                value = ('ushort' / Int16ul).parse(field_data)
                            elif field.type == 'DATE':
                                value = self.to_date(field_data)
                            elif field.type == 'TIME':
                                value = self.to_time(field_data)
                            elif field.type == 'LONG':
                                #TODO
                                if field.name.split(':')[1].lower() in self.date_fieldname:
                                    if Int32sl('long').parse(field_data) == 0:
                                        value = None
                                    else:
                                        value = date.fromordinal(657433 + Int32sl('long').parse(field_data))
                                elif field.name.split(':')[1].lower() in self.time_fieldname:
                                    s, ms = divmod(Int32sl('long').parse(field_data), 100)
                                    value = str('{}.{:03d}'.format(time.strftime('%Y-%m-%d %H:%M:%S',
                                                                                 time.gmtime(s)), ms))
                                else:
                                    value = ('long' / Int32sl).parse(field_data)
                            elif field.type == 'ULONG':
                                value = ('ulong' / Int32ul).parse(field_data)
                            elif field.type == 'FLOAT':
                                value = ('float' / Float32l).parse(field_data)
                            elif field.type == 'DOUBLE':
                                value = ('double' / Float64l).parse(field_data)
                            elif field.type == 'DECIMAL':
                                # TODO BCD
                                if field_data[0] & 0xF0 == 0xF0:
                                    sign = -1
                                    field_data = bytearray(field_data)
                                    field_data[0] &= 0x0F
                                else:
                                    sign = 1
                                value = sign * int(hexlify(field_data)) / 10 ** field.decimal_count
                            elif field.type == 'STRING':
                                value = text_type(field_data, encoding=self.encoding).strip()
                            elif field.type == 'CSTRING':
                                value = text_type(field_data, encoding=self.encoding).strip()
                            elif field.type == 'PSTRING':
                                value = text_type(field_data[1:field_data[0] + 1], encoding=self.encoding).strip()
                            else:
                                # GROUP=0x16
                                # raise ValueError
                                #TODO
                                pass

                            fields[text_type(field.name)] = value
                        # print(fields)
                        yield fields

    def set_current_table(self, tablename):
        print("set_current_table for '"+tablename+"'...")
        self.current_table_number = self.tables.get_number(tablename)
        print('set to', self.current_table_number)
        print(self.tables)

    def to_date(self, value):
        value_date = DATE_STRUCT.parse(value)
        if value_date.year == 0:
            return None
        else:
            return date(value_date.year, value_date.month, value_date.day)

    def to_time(self, value):
        value_time = TIME_STRUCT.parse(value)
        return time(value_time.hour, value_time.minute, value_time.second, value_time.centisecond * 10000)

        # metadata
        # ?header
        # tables (+ record count from metadata)
        # fields
        # longname fields
        #indexes (and key)
        #memos (and blob)

        #records
        #data
        #index

        #other
        #dimension
        #group

# utils
# convert date
# convert time
