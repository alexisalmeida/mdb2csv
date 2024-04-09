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

import sys
from mdb import Mdb


def main(args):
    mdb = Mdb(args[1])

    if mdb:
        print(mdb.list_tables())

        table = mdb.read_table_by_name("SysTable")

        if table:
            table.export("saida.csv")
        else:
            print("Tabela não encontrada")
    else:
        print("Arquivo não encontrado")


if __name__ == '__main__':
    main(sys.argv)
