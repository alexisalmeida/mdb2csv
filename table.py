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

from bind import Bind
from column import Column
from consts import MDB_NUMERIC, MDB_MONEY, MDB_FLOAT, MDB_DOUBLE, MDB_BOOL, MDB_VER_JET3, MDB_INDEX_SCAN, MDB_OLE, \
    MDB_BINARY, MDB_REPID, MDB_PAGE_DATA, OFFSET_MASK, MDB_NOT, MDB_AND, MDB_OR, MDB_ISNULL, MDB_NOTNULL, MDB_BYTE, \
    MDB_INT, MDB_LONGINT, MDB_TEXT, MDB_MEMO, MDB_DATETIME
from field import Field
from utils import get_byte, get_int16, get_int32, is_relational_op, get_single, get_double, test_int, test_double, \
    test_string


class Table:
    def __init__(self, entry):
        self.mdb = entry.mdb
        self.entry = entry       # : MdbCatalogEntry
        self.name = entry.object_name
        self.obj_type = entry.object_type

        self.num_cols = 0
        self.columns = []       # [MdbColumn]
        self.num_rows = 0
        self.index_start = 0
        self.num_real_idxs = 0
        self.num_idxs = 0
        self.indices = []       # [MdbIndex]
        self.first_data_pg = 0
        self.cur_pg_num = 0
        self.cur_phys_pg = 0
        self.cur_row = 0
        self.noskip_del = 0
        self.map_base_pg = 0
        self.map_sz = 0
        self.usage_map = ""
        self.freemap_base_pg = 0
        self.freemap_sz = 0
        self.free_usage_map = ""
        self.sarg_tree = None   # MdbSargNode
        self.strategy = 0       # MdbStrategy
        self.scan_idx = None    # MdbIndex
        self.mdbidx = None
        self.chain = None       # MdbIndexChain
        self.props = None
        self.num_var_cols = 0
        self.is_temp_table = 0
        self.temp_table_pages = []
        self.outfile = None

        self.read_table()

    def read_table(self):
        mdb = self.mdb

        if not mdb.f.read_pg(self.entry.table_pg):
            print(f"mdb_read_table: Unable to read page {self.entry.table_pg}")
            return None

        car = get_byte(mdb.f.pg_buf, 0)
        if car != 0x02:
            print(f"mdb_read_table: Page {self.entry.table_pg} [size={mdb.f.pg_size}] is not a valid table definition "
                  f"page (First byte = 0x{car}, expected 0x02)")
            return None

        # table = mdb_alloc_tabledef(entry)

        get_int16(mdb.f.pg_buf, 8)

        self.num_rows = get_int32(mdb.f.pg_buf, mdb.f.tab_num_rows_offset)
        self.num_var_cols = get_int16(mdb.f.pg_buf, mdb.f.tab_num_cols_offset - 2)
        self.num_cols = get_int16(mdb.f.pg_buf, mdb.f.tab_num_cols_offset)
        self.num_idxs = get_int32(mdb.f.pg_buf, mdb.f.tab_num_idxs_offset)
        self.num_real_idxs = get_int32(mdb.f.pg_buf, mdb.f.tab_num_ridxs_offset)

        # grab a copy of the usage map
        pg_row = get_int32(mdb.f.pg_buf, mdb.f.tab_usage_map_offset)

        result, buf, row_start, self.map_sz = mdb.f.find_pg_row(pg_row)
        if row_start == -1:
            print(f"mdb_read_table: Unable to find page row {pg_row}")
            return None

        # First byte of usage_map is the map-type and must always be present
        if self.map_sz < 1:
            print(f"mdb_read_table: invalid map-size: {self.map_sz}")
            return None

        self.usage_map = buf[row_start: row_start + self.map_sz]

        # grab a copy of the free space page map
        pg_row = get_int32(mdb.f.pg_buf, mdb.f.tab_free_map_offset)

        result, buf, row_start, self.freemap_sz = mdb.f.find_pg_row(pg_row)
        if row_start == -1:
            print(f"mdb_read_table: Unable to find page row {pg_row}")
            return None

        self.free_usage_map = buf[row_start: row_start + self.freemap_sz]

        self.first_data_pg = get_int16(mdb.f.pg_buf, mdb.f.tab_first_dpg_offset)

        if self.entry.props:
            for i in range(len(self.entry.props)):
                props = self.entry.props[i]
                if not props.name:
                    self.props = props

    def export(self, out_file):
        arqs = open(out_file, "w")
        self.outfile = arqs
        delimiter = ";"
        row_delimiter = "\n"
        header_row = True
        null_text = ""
        quote_text = ""

        self.read_columns()
        self.rewind_table()

        bound_values = []
        for i in range(self.num_cols):
            col = self.columns[i]
            col.bind_ptr = Bind(col.name)
            bound_values.append(col.bind_ptr)

        if header_row:
            for i in range(self.num_cols):
                col = self.columns[i]
                if i:
                    arqs.write(delimiter)
                arqs.write(col.name)

            arqs.write(row_delimiter)

        linhas = 0
        while self.fetch_row():
            for i in range(self.num_cols):
                if i > 0:
                    arqs.write(delimiter)

                col = self.columns[i]
                if len(bound_values[i].col_value) == 0:
                    arqs.write(null_text)
                else:
                    value = bound_values[i].col_value

                    self.print_col(value, quote_text, col.col_type)

            arqs.write(row_delimiter)
            linhas += 1

        arqs.close()
        print(linhas)

    def read_columns(self):
        mdb = self.mdb

        self.columns = []

        cur_pos = mdb.f.tab_cols_start_offset + (self.num_real_idxs * mdb.f.tab_ridx_entry_size)

        for i in range(self.num_cols):
            col, cur_pos = mdb.f.read_pg_if_n(cur_pos, mdb.f.tab_col_entry_size)
            if not col:
                return None

            pcol = Column(mdb)
            pcol.table = self
            pcol.col_type = col[0]
            pcol.col_num = col[mdb.f.col_num_offset]
            pcol.var_col_num = get_int16(col, mdb.f.tab_col_offset_var)
            pcol.row_col_num = get_int16(col, mdb.f.tab_row_col_num_offset)

            if pcol.col_type in [MDB_NUMERIC, MDB_MONEY, MDB_FLOAT, MDB_DOUBLE]:
                pcol.col_scale = col[mdb.f.col_scale_offset]
                pcol.col_prec = col[mdb.f.col_prec_offset]

            pcol.is_fixed = 1 if col[mdb.f.col_flags_offset] & 0x01 else 0
            pcol.is_long_auto = 1 if col[mdb.f.col_flags_offset] & 0x04 else 0
            pcol.is_uuid_auto = 1 if col[mdb.f.col_flags_offset] & 0x40 else 0

            pcol.fixed_offset = get_int16(col, mdb.f.tab_col_offset_fixed)

            if pcol.col_type != MDB_BOOL:
                pcol.col_size = get_int16(col, mdb.f.col_size_offset)
            else:
                pcol.col_size = 0

            self.columns.append(pcol)

        # column names - ordered the same as the column attributes table

        for i in range(self.num_cols):
            pcol = self.columns[i]

            is_jet3 = (mdb.f.jet_version == MDB_VER_JET3)

            if is_jet3:
                name_sz, cur_pos = mdb.f.read_pg_if_8(cur_pos)
            else:
                name_sz, cur_pos = mdb.f.read_pg_if_16(cur_pos)

            nome_uni, cur_pos = mdb.f.read_pg_if_n(cur_pos, name_sz)
            if cur_pos > 0:
                pcol.name = mdb.f.unicode2ascii(bytes(nome_uni))

        # Sort the columns by col_num
        # g_ptr_array_sort(table->columns, (GCompareFunc)mdb_col_comparer);

        allprops = self.entry.props
        if allprops:
            for i in range(self.num_cols):
                pcol = self.columns[i]
                for j in range(allprops.len):
                    props = allprops[j]
                    if props.name and props.name == pcol.name:
                        pcol.props = props
                        break
        self.index_start = cur_pos
        return self.columns

    def bind_column_by_name(self, col_bind: Bind):
        if not self.columns:
            return -1
        col_name = col_bind.col_name
        col_num = -1

        for i in range(self.num_cols):
            col = self.columns[i]
            if col.name == col_name:
                col_num = i + 1
                col.bind_ptr = col_bind
                break
        col_bind.col_num = col_num

        return col_num

    def rewind_table(self):
        self.cur_pg_num = 0
        self.cur_phys_pg = 0
        self.cur_row = 0

    def fetch_row(self):
        mdb = self.mdb

        if not self.cur_pg_num:
            self.cur_pg_num = 1
            self.cur_row = 0
            if (not self.is_temp_table) and (self.strategy != MDB_INDEX_SCAN):
                if not self.read_next_dpg():
                    return 0

        while True:
            if self.is_temp_table:
                pages = self.temp_table_pages
                if len(pages) == 0:
                    return 0
                rows = get_int16(pages[self.cur_pg_num - 1], mdb.f.row_count_offset)
                if self.cur_row >= rows:
                    self.cur_row = 0
                    self.cur_pg_num += 1
                    if self.cur_pg_num > len(pages):
                        return 0

                mdb.pg_buf = pages[self.cur_pg_num - 1]
            elif self.strategy == MDB_INDEX_SCAN:
                pass
                """
                pg = mdb_index_find_next(table.mdbidx, table.scan_idx, table.chain, table.cur_row)
                if not pg:
                    return 0
                mdb_read_pg(mdb, pg)
                """
            else:
                rows = get_int16(mdb.f.pg_buf, mdb.f.row_count_offset)

                if self.cur_row >= rows:
                    self.cur_row = 0

                    if not self.read_next_dpg():
                        return 0

            rc = self.read_row(self.cur_row)
            self.cur_row += 1
            if rc:
                break

        return 1

    def print_col(self, col_val, quote_text, col_type):
        is_binary_type = col_type in [MDB_OLE, MDB_BINARY, MDB_REPID]

        if quote_text:
            self.outfile.write(quote_text)

        for c in col_val:
            if is_binary_type:
                self.outfile.write(f"{c}X")
            else:
                self.outfile.write(c)

        if quote_text:
            self.outfile.write(quote_text)

    def read_next_dpg(self):
        # Read next data page into mdb.pg_buf
        entry = self.entry
        mdb = entry.mdb

        while True:
            next_pg = mdb.f.map_find_next(self.usage_map, self.map_sz, self.cur_phys_pg)
            if next_pg < 0:
                break  # unknow map type: goto fallback
            if not next_pg:
                return 0

            if next_pg == self.cur_phys_pg:
                return 0  # Infinite loop

            if not mdb.f.read_pg(next_pg):
                print(f"error: reading page {next_pg} failed.")
                return 0

            self.cur_phys_pg = next_pg

            if mdb.f.pg_buf[0] == MDB_PAGE_DATA and get_int32(mdb.f.pg_buf, 4) == entry.table_pg:
                return self.cur_phys_pg

            # On rare occasion, mdb_map_find_next will return a wrong page
            # Found in a big file, over 4,000,000 records
            print(f"warning: page {next_pg} from map doesn't match: Type={mdb.pg_buf[0]}, "
                  f"buf[4..7]={get_int32(mdb.pg_buf, 4)} Expected table_pg={entry.table_pg}")
        print("Warning: defaulting to brute force read")

        # can't do a fast read, go back to the old way
        while True:
            if not mdb.f.read_pg(self.cur_phys_pg):
                self.cur_phys_pg += 1
                return 0
            self.cur_phys_pg += 1

            if mdb.pg_buf[0] == MDB_PAGE_DATA and get_int32(mdb.pg_buf, 4) == entry.table_pg:
                break

        return self.cur_phys_pg

    def read_row(self, row):
        mdb = self.mdb

        if self.num_cols == 0 or not self.columns:
            return 0

        ret, row_start, row_size = mdb.f.find_row(row)
        if row_start == -1 or row_size == 0:
            return 0

        delflag = lookupflag = 0
        if row_start & 0x8000:
            lookupflag += 1

        if row_start & 0x4000:
            delflag += 1

        row_start &= OFFSET_MASK  # remove flags

        if not self.noskip_del and delflag:
            return 0

        fields = []
        num_fields = self.crack_row(row_start, row_size, fields)

        if num_fields < 0 or not self.test_sargs(fields, num_fields):
            return 0

        # take advantage of mdb_crack_row() to clean up binding
        # use num_cols instead of num_fields -- bsb 03/04/02

        for i in range(len(self.columns)):
            col = self.columns[fields[i].colnum]
            col.attempt_bind(fields[i].is_null, fields[i].start, fields[i].siz)

        return 1

    def crack_row3(self, row_start, row_end, bitmask_sz, row_var_cols):
        mdb = self.mdb
        var_col_offsets = []

        row_len = row_end - row_start + 1
        num_jumps = (row_len - 1) // 256
        col_ptr = row_end - bitmask_sz - num_jumps - 1

        # If last jump is a dummy value, ignore it
        if (col_ptr - row_start - row_var_cols) // 256 < num_jumps:
            num_jumps -= 1

        if (bitmask_sz + num_jumps + 1) > row_end:
            return 0

        if col_ptr >= mdb.f.pg_size or col_ptr < row_var_cols:
            return 0

        jumps_used = 0
        for i in range(row_var_cols + 1):
            while (jumps_used < num_jumps) and i == mdb.pg_buf[row_end - bitmask_sz - jumps_used - 1]:
                jumps_used += 1

            var_col_offsets.append(mdb.f.pg_buf[col_ptr - i] + (jumps_used * 256))

        return 1, var_col_offsets

    def crack_row4(self, row_start, row_end, bitmask_sz, row_var_cols):
        mdb = self.mdb

        if bitmask_sz + 3 + row_var_cols * 2 + 2 > row_end:
            return 0, []

        var_col_offsets = []
        for i in range(row_var_cols + 1):
            var_col_offsets.append(get_int16(mdb.f.pg_buf, row_end - bitmask_sz - 3 - (i * 2)))

        return 1, var_col_offsets

    def crack_row(self, row_start, row_size, fields):
        mdb = self.mdb
        pg_buf = mdb.f.pg_buf
        row_var_cols = 0
        row_end = row_start + row_size - 1

        is_jet3 = (mdb.f.jet_version == MDB_VER_JET3)

        if is_jet3:
            row_cols = get_byte(pg_buf, row_start)
            col_count_size = 1
        else:
            row_cols = get_int16(pg_buf, row_start)
            col_count_size = 2

        bitmask_sz = (row_cols + 7) // 8
        if (bitmask_sz + (0 if is_jet3 else 1)) >= row_end:
            return -1

        nullmask = pg_buf[row_end - bitmask_sz + 1: row_end + 1]

        # read table of variable column locations */
        if self.num_var_cols > 0:
            # row_var_cols = mdb_get_byte(pg_buf, row_end - bitmask_sz) \
            #    if is_jet3 else mdb_get_int16(pg_buf, row_end - bitmask_sz - 1)
            if is_jet3:
                row_var_cols = get_byte(pg_buf, row_end - bitmask_sz)
                success, var_col_offsets = self.crack_row3(row_start, row_end, bitmask_sz, row_var_cols)
            else:
                row_var_cols = get_int16(pg_buf, row_end - bitmask_sz - 1)
                success, var_col_offsets = self.crack_row4(row_start, row_end, bitmask_sz, row_var_cols)

            if not success:
                return -1

        fixed_cols_found = 0
        row_fixed_cols = row_cols - row_var_cols

        for i in range(self.num_cols):
            fields.append(Field(mdb))
            col = self.columns[i]
            fields[i].colnum = i
            fields[i].is_fixed = col.is_fixed
            byte_num = col.col_num // 8
            bit_num = col.col_num % 8
            # logic on nulls is reverse, 1 is not null, 0 is null */
            fields[i].is_null = 0 if nullmask[byte_num] & (1 << bit_num) else 1

            if fields[i].is_fixed and (fixed_cols_found < row_fixed_cols):
                col_start = col.fixed_offset + col_count_size
                fields[i].start = row_start + col_start
                fields[i].value = pg_buf[row_start + col_start:row_start + col_start + col.col_size]
                fields[i].siz = col.col_size
                fixed_cols_found += 1
                # Use col.var_col_num because a deleted column is still
                # present in the variable column offsets table for the row */
            elif not fields[i].is_fixed and (col.var_col_num < row_var_cols):
                col_start = var_col_offsets[col.var_col_num]
                fields[i].start = row_start + col_start
                tam = var_col_offsets[col.var_col_num + 1] - col_start
                fields[i].value = pg_buf[row_start + col_start:row_start + col_start + tam]
                fields[i].siz = tam
            else:
                fields[i].start = 0
                fields[i].value = None
                fields[i].siz = 0
                fields[i].is_null = 1

            if (fields[i].start + fields[i].siz) > row_start + row_size:
                print("warning: Invalid data location detected in mdb_crack_row. Table:{table.name} Column:{i}")
                return -1, None

        return row_cols

    def test_sargs(self, fields, num_fields):
        entry = self.entry
        mdb = self.mdb

        node = self.sarg_tree

        # there may not be a sarg tree
        if not node:
            return 1

        return self.test_sarg_node(node, fields, num_fields)

    def test_sarg_node(self, node, fields, num_fields):
        if is_relational_op(node.op):
            col = node.col

            if not col:
                return node.value.i

            elem = self.find_field(col.col_num, fields, num_fields)

            ret, fields[elem] = self.test_sarg(col, node, elem)
            if not ret:
                return 0
        else:
            if node.op == MDB_NOT:
                rc = self.test_sarg_node(node.left, fields, num_fields)
                return not rc
            elif node.op == MDB_AND:
                if not self.test_sarg_node(node.left, fields, num_fields):
                    return 0
                return self.test_sarg_node(node.right, fields, num_fields)
            elif node.op == MDB_OR:
                if self.test_sarg_node(node.left, fields, num_fields):
                    return 1
                return self.test_sarg_node(node.right, fields, num_fields)

        return 1

    def find_field(self, col_num, fields, num_fields):
        for i in range(num_fields):
            if fields[i].colnum == col_num:
                return i

        return -1

    def test_sarg(self, col, node, field):
        mdb = self.mdb
        ret = 1

        if node.op == MDB_ISNULL:
            ret = field.is_null
        elif node.op == MDB_NOTNULL:
            ret = not field.is_null

        if col.col_type == MDB_BOOL:
            ret = test_int(node, not field.is_null)
        elif col.col_type == MDB_BYTE:
            ret = test_int(node, field.value[0])
        elif col.col_type == MDB_INT:
            ret = test_int(node, get_int16(field.value, 0))
        elif col.col_type == MDB_LONGINT:
            ret = test_int(node, get_int32(field.value, 0))
        elif col.col_type == MDB_FLOAT:
            temp = node.value.i if node.val_type == MDB_INT else node.value.d
            ret = test_double(node.op, temp, get_single(field.value, 0))
        elif col.col_type == MDB_DOUBLE:
            temp = node.value.i if node.val_type == MDB_INT else node.value.d
            ret = test_double(node.op, temp, get_double(field.value, 0))
        elif col.col_type == MDB_TEXT:
            tmpbuf = mdb.f.unicode2ascii(field.value)
            ret = test_string(node, tmpbuf)
        elif col.col_type == MDB_MEMO or col.col_type == MDB_REPID:
            val = col.to_string(mdb.pg_buf, field.start, col.col_type, get_int32(field.value, 0))
            ret = self.test_string(node, val)
        elif col.col_type == MDB_DATETIME:
            ret = self.test_double(node.op, round(node.value.d, 6), round(get_double(field.value, 0), 6))
        else:
            print(f"Calling mdb_test_sarg on unknown type.  Add code to mdb_test_sarg() for type {col.col_type}")

        return ret
