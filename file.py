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

import os.path

from consts import (MDB_WRITABLE, MDB_VER_JET3, MDB_VER_JET4, MDB_VER_ACCDB_2007, MDB_VER_ACCDB_2010,
                    MDB_VER_ACCDB_2013, MDB_VER_ACCDB_2016, MDB_VER_ACCDB_2019, OFFSET_MASK)
from utils import get_byte, mdbi_rc4, get_int16, get_int32, get_single, get_double, decompress_unicode


class MdbFile:
    def __init__(self, mdb, filename):
        self.mdb = mdb
        self.filename = filename
        self.stream = None
        self.writable = False
        self.jet_version = 0
        self.db_key = 0  # [0, 0, 0, 0]
        self.db_passwd = ""  # [14];
        self.map_sz = 0
        self.free_map = ""
        self.refs = 0
        self.code_page = 0
        self.lang_id = 0
        self.pg_buf = ""  # MDB_PGSIZE
        self.alt_pg_buf = ""  # MDB_PGSIZE
        self.cur_pg = 0
        self.cur_pos = 0

        self.pg_size = 2048
        self.row_count_offset = 0x08
        self.tab_num_rows_offset = 12
        self.tab_num_cols_offset = 25
        self.tab_num_idxs_offset = 27
        self.tab_num_ridxs_offset = 31
        self.tab_usage_map_offset = 35
        self.tab_first_dpg_offset = 36
        self.tab_cols_start_offset = 43
        self.tab_ridx_entry_size = 8
        self.col_scale_offset = 9
        self.col_prec_offset = 10
        self.col_flags_offset = 13
        self.col_size_offset = 16
        self.col_num_offset = 1
        self.tab_col_entry_size = 18
        self.tab_free_map_offset = 39
        self.tab_col_offset_var = 3
        self.tab_col_offset_fixed = 14
        self.tab_row_col_num_offset = 5

        self.open()

    def jet3_constants(self):
        self.pg_size = 2048
        self.row_count_offset = 0x08
        self.tab_num_rows_offset = 12
        self.tab_num_cols_offset = 25
        self.tab_num_idxs_offset = 27
        self.tab_num_ridxs_offset = 31
        self.tab_usage_map_offset = 35
        self.tab_first_dpg_offset = 36
        self.tab_cols_start_offset = 43
        self.tab_ridx_entry_size = 8
        self.col_scale_offset = 9
        self.col_prec_offset = 10
        self.col_flags_offset = 13
        self.col_size_offset = 16
        self.col_num_offset = 1
        self.tab_col_entry_size = 18
        self.tab_free_map_offset = 39
        self.tab_col_offset_var = 3
        self.tab_col_offset_fixed = 14
        self.tab_row_col_num_offset = 5

    def jet4_constants(self):
        self.pg_size = 4096
        self.row_count_offset = 0x0c
        self.tab_num_rows_offset = 16
        self.tab_num_cols_offset = 45
        self.tab_num_idxs_offset = 47
        self.tab_num_ridxs_offset = 51
        self.tab_usage_map_offset = 55
        self.tab_first_dpg_offset = 56
        self.tab_cols_start_offset = 63
        self.tab_ridx_entry_size = 12
        self.col_scale_offset = 11
        self.col_prec_offset = 12
        self.col_flags_offset = 15
        self.col_size_offset = 23
        self.col_num_offset = 5
        self.tab_col_entry_size = 25
        self.tab_free_map_offset = 59
        self.tab_col_offset_var = 7
        self.tab_col_offset_fixed = 21
        self.tab_row_col_num_offset = 9

    def open(self):
        filepath = self.filename

        if not os.path.exists(filepath):
            return None

        mode = "rb+" if self.mdb.flags and MDB_WRITABLE else "rb"
        file = open(filepath, mode)

        if not file:
            print(f"Couldn't open file {filepath}\n")
            return None

        return self.handle_from_stream(file)

    def handle_from_stream(self, stream):
        # mdb_set_default_backend(mdb, "access")
        self.refs = 1
        self.stream = stream
        if self.mdb.flags and MDB_WRITABLE:
            self.writable = True

        if not self.read_pg(0):
            print("Couldn't read first page.")
            # mdb_close(mdb)
            return None

        if self.pg_buf[0] != 0:
            return None

        self.jet_version = get_byte(self.pg_buf, 0x14)
        if self.jet_version == MDB_VER_JET3:
            self.jet3_constants()
        elif self.jet_version in [MDB_VER_JET4, MDB_VER_ACCDB_2007, MDB_VER_ACCDB_2010, MDB_VER_ACCDB_2013,
                                  MDB_VER_ACCDB_2016, MDB_VER_ACCDB_2019]:
            self.jet4_constants()
        else:
            print(f"Unknown Jet version: {self.jet_version}")
            return None

        tmp_key = bytes([0xC7, 0xDA, 0x39, 0x6B])
        tam = 126 if self.jet_version == MDB_VER_JET3 else 128
        ret = mdbi_rc4(tmp_key, self.pg_buf[0x18: 0x18 + tam])
        self.pg_buf = self.pg_buf[:0x18] + ret + self.pg_buf[0x18 + tam:]

        if self.jet_version == MDB_VER_JET3:
            self.lang_id = get_int16(self.pg_buf, 0x3a)
        else:
            self.lang_id = get_int16(self.pg_buf, 0x6e)

        self.code_page = get_int16(self.pg_buf, 0x3c)
        self.db_key = get_int32(self.pg_buf, 0x3e)

        if self.jet_version == MDB_VER_JET3:
            # JET4 needs additional masking with the DB creation date, currently unsupported
            # Bug - JET3 supports 20 byte passwords, this is currently just 14 bytes
            self.db_passwd = self.pg_buf[0x42: 0x42 + len(self.db_passwd)]

    def read_pg(self, pg):
        if pg and self.cur_pg == pg:
            return self.pg_size

        self.pg_buf = self._read_pg(pg)
        # print(f"read page {pg} type {self.pg_buf[0]}")
        self.cur_pg = pg
        self.cur_pos = 0
        return len(self.pg_buf)

    def read_alt_pg(self, pg):
        self.alt_pg_buf = self._read_pg(pg)
        return len(self.alt_pg_buf)

    def _read_pg(self, pg):
        offset = pg * self.pg_size

        if self.stream.seek(0, 2) == -1:
            print("Unable to seek to end of file")
            return 0

        if self.stream.tell() < offset:
            print(f"offset {offset} is beyond EOF")
            return 0

        if self.stream.seek(offset) == -1:
            print(f"Unable to seek to page {pg}")
            return 0

        dados = self.stream.read(self.pg_size)

        if pg != 0 and self.db_key != 0:
            tmp_key_i = self.db_key ^ pg

            tmp_key = [tmp_key_i & 0xFF, (tmp_key_i >> 8) & 0xFF,
                       (tmp_key_i >> 16) & 0xFF, (tmp_key_i >> 24) & 0xFF]

            dados = mdbi_rc4(tmp_key, dados)

        return dados

    def pg_get_byte(self, offset):
        if offset < 0 or offset + 1 > self.pg_size:
            return -1

        self.cur_pos += 1
        return self.pg_buf[offset]

    def pg_get_int16(self, offset):
        if offset < 0 or offset + 1 > self.pg_size:
            return -1

        self.cur_pos += 2
        return get_int16(self.pg_buf, offset)

    def pg_get_int32(self, offset):
        if offset < 0 or offset + 1 > self.pg_size:
            return -1

        self.cur_pos += 4
        return get_int32(self.pg_buf, offset)

    def pg_get_single(self, offset):
        if offset < 0 or offset + 1 > self.pg_size:
            return -1

        self.cur_pos += 4

        return get_single(self.pg_buf, offset)

    def pg_get_double(self, offset):
        if offset < 0 or offset + 8 > self.pg_size:
            return -1

        self.cur_pos += 8
        return get_double(self.pg_buf, offset)

    def set_pos(self, pos):
        if pos < 0 or pos >= self.pg_size:
            return 0

        self.cur_pos = pos
        return pos

    def get_pos(self):
        return self.cur_pos

    def swap_pgbuf(self):
        tmpbuf = self.pg_buf
        self.pg_buf = self.alt_pg_buf
        self.alt_pg_buf = tmpbuf

    def find_pg_row(self, pg_row):
        """
         * mdb_find_pg_row
         * @mdb: Database file handle
         * @pg_row: Lower byte contains the row number, the upper three contain page
         * @buf: Pointer for returning a pointer to the page
         * @off: Pointer for returning an offset to the row
         * @len: Pointer for returning the length of the row
         *
         * Returns: 0 on success. -1 on failure.
        """

        pg = pg_row >> 8
        row = pg_row & 0xff

        if self.read_alt_pg(pg) != self.pg_size:
            return -1, -1, -1, None

        self.swap_pgbuf()
        result, off, tam = self.find_row(row)
        self.swap_pgbuf()
        off &= OFFSET_MASK
        buf = self.alt_pg_buf
        return result, buf, off, tam

    def find_row(self, row):
        rco = self.row_count_offset

        if row > 1000:
            return -1, -1, 0

        start = get_int16(self.pg_buf, rco + 2 + row * 2)
        next_start = self.pg_size if row == 0 else get_int16(self.pg_buf, rco + row * 2) & OFFSET_MASK
        tam = next_start - (start & OFFSET_MASK)

        if ((start & OFFSET_MASK) >= self.pg_size or (start & OFFSET_MASK) > next_start or
                next_start > self.pg_size):
            return -1, -1, 0

        return 0, start, tam

    def read_pg_if_n(self, cur_pos, tam):
        buf = []

        if cur_pos < 0:
            return None

        # Advance to page which contains the first byte
        while cur_pos >= self.pg_size:
            if not self.read_pg(get_int32(self.pg_buf, 4)):
                return None
            cur_pos -= (self.pg_size - 8)

        # Copy pages into buffer
        while (cur_pos + tam) >= self.pg_size:
            piece_len = self.pg_size - cur_pos

            buf += self.pg_buf[cur_pos: cur_pos + piece_len]

            tam -= piece_len
            if not self.read_pg(get_int32(self.pg_buf, 4)):
                return None
            cur_pos = 8

        # Copy into buffer from final page
        if tam:
            buf += self.pg_buf[cur_pos: cur_pos + tam]

        cur_pos += tam

        return buf, cur_pos

    def read_pg_if_32(self, cur_pos):
        c, cur_pos = self.read_pg_if_n(cur_pos, 4)
        return get_int32(c, 0), cur_pos

    def read_pg_if_16(self, cur_pos):
        c, cur_pos = self.read_pg_if_n(cur_pos, 2)
        return get_int16(c, 0), cur_pos

    def read_pg_if_8(self, cur_pos):
        c, cur_pos = self.read_pg_if_n(cur_pos, 1)
        return c[0], cur_pos

    def unicode2ascii(self, src):
        is_jet3 = (self.jet_version == MDB_VER_JET3)
        slen = len(src) - 2

        if not is_jet3 and slen >= 2 and (src[0] & 0xff) == 0xff and (src[1] & 0xff) == 0xfe:
            tmp = [0] * (slen * 2)
            decompress_unicode(src[2:], tmp)
            src = bytes(tmp)

        if is_jet3:
            saida = src.decode('utf-8')
        else:
            saida = src.decode('utf-16')
        # ascii_str = unicode_str.encode('ascii', 'ignore').decode('ascii')

        return saida  # bytes(ascii_str)

    def map_find_next0(self, p_map, map_sz, start_pg):
        if map_sz < 5:
            return 0

        pgnum = get_int32(p_map, 1)
        usage_bitmap = 5  # map[5:]
        usage_bitlen = (map_sz - 5) * 8

        ini = start_pg - pgnum + 1 if start_pg >= pgnum else 0
        for i in range(ini, usage_bitlen):
            # if usage_bitmap[i/8] & (1 << (i%8)):
            if p_map[usage_bitmap + i // 8] & (1 << (i % 8)):
                return pgnum + i

        return 0

    def map_find_next1(self, p_map, map_sz, start_pg):
        """
        * start_pg will tell us where to (re)start the scan
        * for the next data page.  each usage_map entry points to a
        * 0x05 page which bitmaps (mdb.fmt.pg_size - 4) * 8 pages.
        *
        * map_ind gives us the starting usage_map entry
        * offset gives us a page offset into the bitmap
        """

        usage_bitlen = (self.pg_size - 4) * 8
        max_map_pgs = (map_sz - 1) / 4
        map_ind = (start_pg + 1) / usage_bitlen
        offset = (start_pg + 1) % usage_bitlen

        while map_ind < max_map_pgs:
            map_pg = get_int32(p_map, (map_ind * 4) + 1)
            if not map_pg:
                continue

            if self.read_alt_pg(map_pg) != self.pg_size:
                print(f"Oops! didn't get a full page at {map_pg}")
                return -1

            usage_bitmap = self.alt_pg_buf[4:]
            for i in range(offset, usage_bitlen):
                if usage_bitmap[i // 8] & (1 << (i % 8)):
                    return map_ind * usage_bitlen + i

            offset = 0
            map_ind += 1

        # didn't find anything
        return 0

    # returns 0 on EOF
    # returns -1 on error (unsupported map type)
    def map_find_next(self, p_map, map_sz, start_pg):
        if p_map[0] == 0:
            return self.map_find_next0(p_map, map_sz, start_pg)
        elif p_map[0] == 1:
            return self.map_find_next1(p_map, map_sz, start_pg)

        print(f"Warning: unrecognized usage map type: {p_map[0]}")
        return -1
