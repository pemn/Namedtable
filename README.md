# Namedtable
Object oriented library for tables with header (tabular data where the first row in the column name). Tipical case is csv files.  
Provides many tools for data manipulation and statistics.  
Available as a standalone perl module. All required modules are standard in most perl distros.  
Can be used as guideline for creating a equivalent library in other interpreted languages.  

## Features
- Multiple input formats: csv,txt,prn,xls,xlsx and some proprietary formats (bmf,dm,dmp)
- Multiple output formats: csv,txt,prn,xls,xlsx and zip (stream compressed csv)
- Can handle very large datasets which would exceed memory addressing limits using a optional "Tie File" interface.
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
