# Namedtable
Object oriented library for tables with header (tabular data where the first row in the column name). Tipical case is csv files.  
Provides many tools for data manipulation and statistics. Some of these tools are very hard to find even in commercial packages.  
Available as a standalone perl module. All required modules are standard in most perl distros.  
Can be used as guideline for creating a equivalent library in other interpreted languages.  
Its similar in concept and features to pythons pandas Dataframe library.

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
- SQL-Like joins between two tables. Left,Right,Inner,Outer
- Interval join. Join tables where data is delimited by both primary key and a distance field. Ex.: Drillholes, Roads, Historic Events

## Examples
Input: `sales.csv`  

| Id | Client Name | Client Gender | Product | Quantity | Price |  
| --- | --- | --- | --- | --- | --- |
| 0 | John | Male | Bread | 2 | 10 |  
| 1 | John | Male | Soda | 1 | 8 |  
| 2 | Mary | Female | Tea | 1 | 4 |  
| 3 | Bob | Male | Beer | 4 | 5 |  
| 4 | Ann | Female | Bread | 1 | 10 |  
| 5 | Lucy | Female | Beer | 2 | 5 |  
| 6 | Joe | Male | Cake | 1 | 20 |  

Example of output, sales report:  

| Product | Quantity |  
| --- | --- |  
| Beer |  6  |  
| Bread |  3  |  
| Cake |  1  |  
| Soda |  1  |  
| Tea |  1  |  

Example of output, Average product price purchased, by gender:  

| Client Gender	| average product price |
| --- | --- |
| Male |	8.5 |
| Female	| 6 |

More examples with code in `examples`

