# Namedtable
Object oriented library for tables with header (tabular data where the first row in the column name). Tipical case is csv files.  
Provides many tools for data manipulation and statistics.  
Available as a standalone perl module. All required modules are standard in most perl distros.  
Can be used as guideline for creating a equivalent library in other interpreted languages.  

## Features
- Multiple input formats: csv,txt,prn,xls,xlsx and some proprietary formats (bmf,dm,dmp)
- Multiple output formats: csv,txt,prn,xls,xlsx and zip (stream compressed csv)
- Dynamic dependency loading. Ex.: Excel will be required only if you are reading/writing a xls file.
- Optional output as a "new" Excel sheet, which exists only in memory. User can then "Save As..." or close.
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
- Synonyms. Columns may have multiple variants. Ex.: ['City','Site','Town','Location']

## Examples
| --- | --- |  
| Id | Client Name | Client Gender | Product | Quantity | Price |  
| --- | --- |  
| 0 | John | Male | Bread | 2 | 10 |  
| --- | --- |  
| 1 | John | Male | Soda | 1 | 8 |  
| --- | --- |  
| 2 | Mary | Female | Tea | 1 | 4 |  
| --- | --- |  
| 3 | Bob | Male | Beer | 4 | 5 |  
| --- | --- |  
| 4 | Ann | Female | Bread | 1 | 10 |  
| --- | --- |  
| 5 | Lucy | Female | Beer | 2 | 5 |  
| --- | --- |  
| 6 | Joe | Male | Cake | 1 | 20 |  
