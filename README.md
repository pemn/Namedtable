# Namedtable
Object oriented library for headered tables. Provides many tools for data manipulation and statistics.

## Features
- Multiple input filters: csv,txt,prn,xls,xlsx and some proprietary formats (bmf,dm,dmp)
- Multiple output filters: csv,txt,prn,xls,xlsx andzip (stream compressed csv)
- Filter data using a condition. Ex.: 
 - `year > 2010`
 - `version > 2.0 and type eq 'software'`
 - `salary > 0 or status eq 'voluntary'`
- Industrial grade statistics package: 
 - breakdown by value(s) ("pivot table")
 - median / weighted median
 - mean / weighted mean
 - sum / sumproduct
 - count
 - major / weighted major
 - concatenate list
- Evaluate expressions
 - Create new columns automatically
 - Change value of row data using formulas
 - Call any functions visible in the current scope with the current row data
