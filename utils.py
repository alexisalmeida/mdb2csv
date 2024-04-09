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

import struct
from datetime import datetime

from arc4 import ARC4

from consts import MDB_EQUAL, MDB_LT, MDB_GTEQ, MDB_LTEQ, MDB_NEQ, MDB_LIKE, MDB_ILIKE, MDB_ISNULL, MDB_NOTNULL, MDB_GT, \
    MDB_BYTE, MDB_LONGINT, MDB_COMPLEX, MDB_FLOAT, MDB_DOUBLE, MDB_BINARY, MDB_TEXT, MDB_INT, MDB_DATETIME, MDB_MEMO, \
    MDB_MONEY


def get_byte(buf, offset):
    return buf[offset]


def get_int16(buf, offset):
    u8_buf = buf[offset: offset + 2]
    return (u8_buf[0] << 0) + (u8_buf[1] << 8)


def get_int32(buf, offset):
    u8_buf = buf[offset: offset + 4]

    return (u8_buf[0] << 0) + (u8_buf[1] << 8) + (u8_buf[2] << 16) + (u8_buf[3] << 24)


def get_int32_msb(buf, offset):
    u8_buf = buf[offset: offset + 4]
    return (u8_buf[0] << 24) + (u8_buf[1] << 16) + (u8_buf[2] << 8) + (u8_buf[3] << 0)


def get_single(buf, offset):
    u8_buf = buf[offset:offset + 4]

    return struct.unpack('f', u8_buf)[0]


def get_double(buf, offset):
    u8_buf = buf[offset: offset + 8]

    num_double = struct.unpack('d', u8_buf)[0]

    # return ((u8_buf[0] << 0) + (u8_buf[1] << 8) + (u8_buf[2] << 16) + (u8_buf[3] << 24) +
    #        (u8_buf[4] << 32) + (u8_buf[5] << 40) + (u8_buf[6] << 48) + (u8_buf[7] << 56))

    return int(num_double)


def mdbi_rc4(key, buf):
    arc4 = ARC4(key)
    tam = len(buf)
    cipher = arc4.encrypt(buf[:tam])
    return cipher


def decompress_unicode(src, dst):
    """
     * This function is used in reading text data from an MDB table.
     * 'dest' will receive a converted, null-terminated string.
     * dlen is the available size of the destination buffer.
     * Returns the length of the converted string, not including the terminator.
    """
    compress = 1
    tlen = 0
    slen = len(src)
    dlen = len(dst)
    isrc = 0
    while slen > 0 and tlen < dlen:
        if src[isrc] == 0:
            compress = 0 if compress else 1
            isrc += 1
            slen -= 1
        elif compress:
            dst[tlen] = src[isrc]
            tlen += 1
            isrc += 1
            dst[tlen] = 0
            tlen += 1
            slen -= 1
        elif slen >= 2:
            dst[tlen] = src[isrc]
            tlen += 1
            isrc += 1
            dst[tlen] = src[isrc]
            tlen += 1
            isrc += 1
            slen -= 2
        else:
            break

    return tlen


def date_to_string(fmt, buf, start):
    td = get_double(buf, start)
    t = date_to_tm(td)
    dt_hr = datetime(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)
    text = dt_hr.strftime(fmt)

    return text


class Tm:
    tm_sec = 0         # Segundos (0-59)
    tm_min = 0         # Minutos (0-59)
    tm_hour = 0        # Horas (0-23)
    tm_mday = 0        # Dia do mês (1-31)
    tm_mon = 0         # Mês (0-11)
    tm_year = 0        # Ano - 1900
    tm_wday = 0        # Dia da semana (0-6, onde 0 é Domingo)
    tm_yday = 0        # Dia do ano (0-365, onde 0 é 1 de janeiro)
    tm_isdst = 0       # Flag de horário de verão (positivo, negativo ou zero)

noleap_cal = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
leap_cal = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366]


def date_to_tm(td):
    t = Tm()

    if td < 0.0 or td > 1e6:
        return

    yr = 1
    day = td
    time = (td - day) * 86400.0 + 0.5
    t.tm_hour = time / 3600
    t.tm_min = (time / 60) % 60
    t.tm_sec = time % 60

    day += 693593  # Days from 1/1/1 to 12/31/1899
    t.tm_wday = (day + 1) % 7

    q = day / 146097  # 146097 days in 400 years
    yr += 400 * q
    day -= q * 146097

    q = day / 36524  # 36524 days in 100 years
    if q > 3:
        q = 3
    yr += 100 * q
    day -= q * 36524

    q = day / 1461  # 1461 days in 4 years
    yr += 4 * q
    day -= q * 1461

    q = day / 365  # 365 days in 1 year
    if q > 3:
        q = 3
    yr += q
    day -= q * 365

    cal = leap_cal if (yr % 4 == 0 and (yr % 100 != 0 or yr % 400 == 0)) else noleap_cal

    for t.tm_mon in range(12):
        if day < cal[t.tm_mon + 1]:
            break

    t.tm_year = yr - 1900
    t.tm_mday = day - cal[t.tm_mon] + 1
    t.tm_yday = day
    t.tm_isdst = -1

    return t


def is_relational_op(x):
    return x in [MDB_EQUAL, MDB_GT, MDB_LT, MDB_GTEQ, MDB_LTEQ, MDB_NEQ, MDB_LIKE, MDB_ILIKE,
                 MDB_ISNULL, MDB_NOTNULL]


def test_int(node, i):
    val = node.value.i if node.val_type == MDB_INT else node.value.d

    if node.op == MDB_EQUAL:
        if val == i:
            return 1
    elif node.op == MDB_GT:
        if val < i:
            return 1
    elif node.op == MDB_LT:
        if val > i:
            return 1
    elif node.op == MDB_GTEQ:
        if val <= i:
            return 1
    elif node.op == MDB_LTEQ:
        if val >= i:
            return 1
    elif node.op == MDB_NEQ:
        if val != i:
            return 1
    else:
        print(f"Calling mdb_test_sarg on unknown operator.  Add code to mdb_test_int() for operator {node.op}")

    return 0


def test_double(op, vd, d):
    if op == MDB_EQUAL:
        ret = (vd == d)
    elif op == MDB_GT:
        ret = (vd < d)
    elif op == MDB_LT:
        ret = (vd > d)
    elif op == MDB_GTEQ:
        ret = (vd <= d)
    elif op == MDB_LTEQ:
        ret = (vd >= d)
    elif op == MDB_NEQ:
        ret = (vd != d)
    else:
        print(f"Calling mdb_test_sarg on unknown operator.  Add code to mdb_test_double() for operator {op}")
        ret = False

    return ret


def test_string(node, s):
    if node.op == MDB_LIKE:
        return like_cmp(s, node.value.s)
    if node.op == MDB_ILIKE:
        return ilike_cmp(s, node.value.s)
    if node.op == MDB_EQUAL:
        return s == node.value.s
    elif node.op == MDB_GT:
        return s > node.value.s
    elif node.op == MDB_LT:
        return s < node.value.s
    elif node.op == MDB_GTEQ:
        return s >= node.value.s
    elif node.op == MDB_LTEQ:
        return s <= node.value.s
    elif node.op == MDB_NEQ:
        return s != node.value.s
    else:
        print(f"Calling mdb_test_sarg on unknown operator.  Add code to mdb_test_string() for operator {node.op}")

    return 0


def like_cmp(s, r):
    """
     *
     * @param s: String to search within.
     * @param r: Search pattern.
     *
     * Tests the string @s to see if it matches the search pattern @r.  In the
     * search pattern, a percent sign indicates matching on any number of
     * characters, and an underscore indicates matching any single character.
     *
     * @Returns: 1 if the string matches, 0 if the string does not match.
    """
    if r == "":
        return s == ""

    if r[0] == '_':
        return like_cmp(s[1:], r[1:])
    elif r[0] == '%':
        for i in range(len(s)+1):
            if like_cmp(s[i:], r[1:]):
                return True
        return False
    else:
        for i in range(len(r)):
            if r[i] == '_' or r[i] == '%':
                break

            if s[i] != r[i]:
                return False
            else:
                ret = like_cmp(s[i+1:], r[i+1:])
                return ret

    return False


def ilike_cmp(s, r):
    return like_cmp(s.upper(), r.upper())
