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

from consts import MDB_TABLE


class Catalog:
    def __init__(self, mdb):
        self.mdb = mdb
        self.object_name = ""  # [MDB_MAX_OBJ_NAME + 1]
        self.object_type = 0
        self.table_pg = 0
        self.props = {}  # MdbProperties
        self.flags = 0

    def is_user_table(self):
        return self.object_type == MDB_TABLE and not (self.flags & 0x80000002)
