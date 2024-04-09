"""
    mdb2csv - Exports tables from mdb file to csv file
    Copyright (C) 2024  Aléxis Rodrigues de Almeida

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

from consts import (MDB_BOOL, MDB_OLE, MDB_NUMERIC, MDB_DATETIME, MDB_BYTE, MDB_INT, MDB_LONGINT,
                    MDB_COMPLEX, MDB_FLOAT, MDB_DOUBLE, MDB_BINARY, MDB_TEXT, MDB_MEMO, MDB_MONEY,
                    MDB_MEMO_OVERHEAD, MDB_REPID)
from utils import date_to_string, get_byte, get_int16, get_int32, get_single, get_double


class Column:
    def __init__(self, mdb):
        self.mdb = mdb
        self.table: None  # S_MdbTableDef
        self.name = ""  # [MDB_MAX_OBJ_NAME + 1];
        self.col_type = 0
        self.col_size = 0
        self.bind_ptr = None
        self.properties = None  # GHashTable
        self.num_sargs = 0
        self.sargs = []
        self.idx_sarg_cache = []
        self.is_fixed = 0
        self.query_order = 0
        self.col_num = 0
        self.cur_value_start = 0
        self.cur_value_len = 0
        self.cur_blob_pg_row = 0
        self.chunk_size = 0
        self.col_prec = 0
        self.col_scale = 0
        self.is_long_auto = 0
        self.is_uuid_auto = 0
        self.props = None  # : MdbProperties
        self.fixed_offset = 0
        self.var_col_num = 0
        self.row_col_num = 0

    def attempt_bind(self, isnull, offset, tam):
        if self.col_type == MDB_BOOL:
            self.xfer_bound_bool(isnull)
        elif isnull:
            self.xfer_bound_data(0, 0)
        elif self.col_type == MDB_OLE:
            pass
        else:
            self.xfer_bound_data(offset, tam)

        return 1

    def xfer_bound_bool(self, value):
        mdb = self.mdb
        self.cur_value_len = value
        if self.bind_ptr:
            self.bind_ptr.col_value = mdb.boolean_false_value if value else mdb.boolean_true_value

        return 1

    def xfer_bound_data(self, start, tam):
        if tam:
            self.cur_value_start = start
            self.cur_value_len = tam
        else:
            self.cur_value_start = 0
            self.cur_value_len = 0

        if self.bind_ptr:
            if not tam:
                self.bind_ptr.col_value = ""
            else:
                if self.col_type == MDB_NUMERIC:
                    text = self.numeric_to_string(start)
                elif self.col_type == MDB_DATETIME:
                    if self.is_shortdate():
                        text = date_to_string(self.mdb.shortdate_fmt, self.mdb.f.pg_buf, start)
                    else:
                        text = date_to_string(self.mdb.date_fmt, self.mdb.f.pg_buf, start)
                else:
                    text = self.to_string(self.mdb.f.pg_buf, start, tam)

                self.bind_ptr.col_value = text

            return len(self.bind_ptr.col_value)

        return 0

    def is_shortdate(self):
        fmt = self.get_prop("Format")
        return fmt and fmt == "Short Date"

    def get_prop(self, key):
        if not self.props:
            return None

        return self.props[key]

    def memo_to_string(self, start, size):
        mdb = self.mdb
        buf = mdb.f.pg_buf
        pg_buf = buf

        if size < MDB_MEMO_OVERHEAD:
            return ""

        # The 32 bit integer at offset 0 is the length of the memo field
        #   with some flags in the high bits.
        # The 32 bit integer at offset 4 contains page and row information.

        memo_len = get_int32(pg_buf, start)

        if memo_len & 0x80000000:
            # inline memo field
            temp = pg_buf[start + MDB_MEMO_OVERHEAD: start + MDB_MEMO_OVERHEAD + (size - MDB_MEMO_OVERHEAD)]
            text = self.mdb.f.unicode2ascii(temp)
            return text
        elif memo_len & 0x40000000:
            # single-page memo field
            pg_row = get_int32(pg_buf, start + 4)
            ret, buf, row_start, tam = mdb.f.find_pg_row(pg_row)
            if ret == -1:
                return ""

            temp = buf[row_start, row_start + tam]
            text = mdb.f.unicode2ascii(mdb, temp)
            return text
        elif (memo_len & 0xff000000) == 0:  # assume all flags in MSB
            # multi-page memo field
            tmp = []
            tmpoff = 0

            pg_row = get_int32(pg_buf, start + 4)
            while True:
                ret, buf, row_start, tam = self.mdb.f.find_pg_row(pg_row)
                if not row_start:
                    return ""

                if (tmpoff + tam - 4) > memo_len:
                    break

                # Stop processing on zero length multiple page memo fields
                if tam < 4:
                    break

                tmp += buf[row_start + 4: row_start + 4 + tam - 4]
                tmpoff += tam - 4

                pg_row = get_int32(buf, row_start)

                if not pg_row:
                    break

            if tmpoff < memo_len:
                print("Warning: incorrect memo length")

            text = self.mdb.f.unicode2ascii(tmp)
            return text
        else:
            print(f"Unhandled memo field flags = {memo_len >> 24}")
            return ""

    def to_string(self, buf, start, size):
        datatype = self.col_type

        if datatype == MDB_BYTE:
            text = str(get_byte(buf, start))
        elif datatype == MDB_INT:
            text = str(get_int16(buf, start))
        elif datatype in [MDB_LONGINT, MDB_COMPLEX]:
            text = str(get_int32(buf, start))
        elif datatype == MDB_FLOAT:
            tf = get_single(buf, start)
            text = str(tf).split(".")[0]
        elif datatype == MDB_DOUBLE:
            td = get_double(buf, start)
            text = str(td).split(".")[0]
        elif datatype == MDB_BINARY:
            if size < 0:
                text = ""
            else:
                text = buf[start: start + size]
        elif datatype == MDB_TEXT:
            if size < 0:
                text = ""
            else:
                text = self.mdb.f.unicode2ascii(buf[start: start + size])
        elif datatype == MDB_DATETIME:
            text = date_to_string(self.mdb.date_fmt, buf, start)
        elif datatype == MDB_MEMO:
            text = self.memo_to_string(start, size)
        elif datatype == MDB_MONEY:
            # text = mdb_money_to_string(mdb, start)
            text = ""
            pass
        elif datatype == MDB_REPID:
            # text = mdb_uuid_to_string_fmt(buf, start, mdb.repid_fmt)
            text = ""
            pass
        else:
            print(f"Warning: mdb_col_to_string called on unsupported data type {datatype}")
            text = ""

        return text

    def numeric_to_string(self, start):
        #   scale = self.col_scale
        #   prec = self.col_prec
        num_bytes = 16
        b = self.mdb.f.pg_buf[start + 1: start + 1 + num_bytes]

        s = 0
        for i in range(num_bytes):
            s = s * 16 + b[i]

        return str(s)
