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
from consts import MDB_BIND_SIZE, boolean_false_number, boolean_true_number, MDB_BRACES_4_2_2_8, MDB_ANY
from file import MdbFile
from catalog import Catalog
from consts import MDB_TABLE
from table import Table


class Mdb:
    def __init__(self, filename):
        self.guint32 = 0
        self.guint16 = 0
        self.row_num = 0
        self.size_t = 0
        self.bind_size = 0
        self.date_fmt = ""  # 64
        self.shortdate_fmt = ""  # 64
        self.repid_fmt = None
        self.boolean_false_value = False
        self.boolean_true_value = True
        self.num_catalog = 0
        self.catalog = []  # MdbCatalogEntry
        self.flags = 0

        self.date_fmt = "%x %X"
        self.shortdate_fmt = "%x"
        self.bind_size = MDB_BIND_SIZE
        self.boolean_false_value = boolean_false_number
        self.boolean_true_value = boolean_true_number

        self.repid_fmt = MDB_BRACES_4_2_2_8

        self.f = MdbFile(self, filename)

        self.read_catalog(MDB_TABLE)

    def read_table_by_name(self, name):
        for i in range(self.num_catalog):
            entry = self.catalog[i]
            if entry.object_name == name:
                table = Table(entry)
                return table

        return None

    def list_tables(self):
        result = []
        for i in range(self.num_catalog):
            entry: Catalog = self.catalog[i]
            result.append({"name": entry.object_name, "type": entry.object_type, "flags": entry.flags,
                           "user_table": entry.is_user_table()})

        return result

    def read_catalog(self, objtype):
        msysobj = Catalog(self)
        msysobj.object_type = MDB_TABLE
        msysobj.table_pg = 2
        msysobj.object_name = "MSysObjects"

        table = Table(msysobj)
        if not table:
            print(f"Unable to read table {msysobj.object_name}")
            return None

        if not table.read_columns():
            print(f"Unable to read columns of table {msysobj.object_name}")
            return None

        obj_id = Bind('Id')
        obj_name = Bind('Name')
        obj_type = Bind('Type')
        obj_flags = Bind('Flags')
        table.bind_column_by_name(obj_id)
        table.bind_column_by_name(obj_name)
        table.bind_column_by_name(obj_type)
        table.bind_column_by_name(obj_flags)

        if obj_id.col_num == -1 or obj_name.col_num == -1 or obj_type.col_num == -1 or obj_flags.col_num == -1:
            print(f"Unable to bind columns from table {msysobj.object_name} ({table.num_cols} columns found)")
            return None

        i = Bind("LvProp")
        table.bind_column_by_name(i)
        if i.col_num == -1:
            print(f"Unable to bind column LvProp from table {msysobj.object_name}")
            return None

        table.rewind_table()

        while table.fetch_row():
            local_type = int(obj_type.col_value)
            if objtype == MDB_ANY or local_type == objtype:
                entry = Catalog(self)
                entry.object_name = obj_name.col_value
                entry.object_type = local_type & 0x7F
                entry.table_pg = (int(obj_id.col_value) & 0x00FFFFFF) if obj_id.col_value else -1
                entry.flags = int(obj_flags.col_value) if obj_flags.col_value else -1
                self.num_catalog += 1
                self.catalog.append(entry)
