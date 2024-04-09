# mdb2csv

This library implements functions that allow you to list and export tables from an mdb file, both jet3 and newer versions.

The programs were, for the most part, converted from C to Python using the mdbtools source codes, authored by Brian Bruns and others. (https://github.com/mdbtools/mdbtools)

They were also transformed into an object model to facilitate their use and maintenance by Python programmers.

Initially, only the functions strictly necessary for the primary objective were converted. This primary objective was to export tables to a csv file so that it can be imported into other platforms.

Therefore, the functions relating to recording the mdb or exploring via SQL commands were not converted.
