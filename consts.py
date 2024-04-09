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

MDB_NOFLAGS = 0x00
MDB_WRITABLE = 0x01
MDB_BIND_SIZE = 16384
MDB_BRACES_4_2_2_8 = "{XXXX-XX-XX-XXXXXXXX}"
MDB_NOBRACES_4_2_2_2_6 = "XXXX-XX-XX-XX-XXXXXX"
MDB_VER_JET3 = 0
MDB_VER_JET4 = 0x01
MDB_VER_ACCDB_2007 = 0x02
MDB_VER_ACCDB_2010 = 0x03
MDB_VER_ACCDB_2013 = 0x04
MDB_VER_ACCDB_2016 = 0x05
MDB_VER_ACCDB_2019 = 0x06

MDB_FORM = 0
MDB_TABLE = 1
MDB_MACRO = 2
MDB_SYSTEM_TABLE = 3
MDB_REPORT = 4
MDB_QUERY = 5
MDB_LINKED_TABLE = 6
MDB_MODULE = 7
MDB_RELATIONSHIP = 8
MDB_UNKNOWN_09 = 9
MDB_UNKNOWN_0A = 10
MDB_DATABASE_PROPERTY = 11
MDB_ANY = -1

MDB_TABLE_SCAN = 0
MDB_LEAF_SCAN = 1
MDB_INDEX_SCAN = 2

MDB_PAGE_DB = 0
MDB_PAGE_DATA = 1
MDB_PAGE_TABLE = 2
MDB_PAGE_INDEX = 3
MDB_PAGE_LEAF = 4
MDB_PAGE_MAP = 5

MDB_BOOL = 0x01
MDB_BYTE = 0x02
MDB_INT = 0x03
MDB_LONGINT = 0x04
MDB_MONEY = 0x05
MDB_FLOAT = 0x06
MDB_DOUBLE = 0x07
MDB_DATETIME = 0x08
MDB_BINARY = 0x09
MDB_TEXT = 0x0a
MDB_OLE = 0x0b
MDB_MEMO = 0x0c
MDB_REPID = 0x0f
MDB_NUMERIC = 0x10
MDB_COMPLEX = 0x12

MDB_OR = 1
MDB_AND = 2
MDB_NOT = 3
MDB_EQUAL = 4
MDB_GT = 5
MDB_LT = 6
MDB_GTEQ = 7
MDB_LTEQ = 8
MDB_LIKE = 9
MDB_ISNULL = 10
MDB_NOTNULL = 11
MDB_ILIKE = 12
MDB_NEQ = 13

MDB_DEBUG_LIKE = 0x0001
MDB_DEBUG_WRITE = 0x0002
MDB_DEBUG_USAGE = 0x0004
MDB_DEBUG_OLE = 0x0008
MDB_DEBUG_ROW = 0x0010
MDB_DEBUG_PROPS = 0x0020
MDB_USE_INDEX = 0x0040
MDB_NO_MEMO = 0x0080

boolean_false_number = "0"
boolean_true_number = "1"

boolean_false_word = "FALSE"
boolean_true_word = "TRUE"

OFFSET_MASK = 0x1fff
OLE_BUFFER_SIZE = MDB_BIND_SIZE*64
MDB_MAX_IDX_COLS = 10
MDB_MEMO_OVERHEAD = 12

MDB_BINEXPORT_STRIP = 0
MDB_BINEXPORT_RAW = 1
MDB_BINEXPORT_OCTAL = 2
MDB_BINEXPORT_HEXADECIMAL = 3

"""
#define MDB_PGSIZE 4096
//#define MDB_MAX_OBJ_NAME (256*3) /* unicode 16 -> utf-8 worst case */
#define MDB_MAX_OBJ_NAME 256
#define MDB_MAX_COLS 256
#define MDB_MAX_IDX_COLS 10
#define MDB_CATALOG_PG 18
"""


def mdb_is_relational_op(x):
    return x in [MDB_EQUAL, MDB_GT, MDB_LT, MDB_GTEQ, MDB_LTEQ, MDB_NEQ, MDB_LIKE, MDB_ILIKE,
                 MDB_ISNULL, MDB_NOTNULL]
