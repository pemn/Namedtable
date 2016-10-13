# Examples

Some examples using the least possible simple code.  
You must have the file Namedtable.pm in the current working directory (or somewhere the perl module search can find) for the examples to run.
You can see the examples input and output just by opening them here in github.

## Example 1
Group data by year, weighted by kms
`perl seatbelts_statistics.pl`  
Input: [seatbelts.csv](https://github.com/pemn/Namedtable/blob/master/examples/seatbelts.csv)  
Output: [seatbelts_statistics_output.csv](https://github.com/pemn/Namedtable/blob/master/examples/seatbelts_statistics_output.csv)  

## Example 2
Income per capta only on states where income > 5000  
`perl state_x77_condition.pl`  
Input: [state_x77.csv](https://github.com/pemn/Namedtable/blob/master/examples/state_x77.csv)  
Output: [state_x77_condition_output.csv](https://github.com/pemn/Namedtable/blob/master/examples/state_x77_condition_output.csv)  

## Example 3
Create a new column with the sum of two existing columns  
`perl seatbelts_evaluate.pl`  
Input: [seatbelts.csv](https://github.com/pemn/Namedtable/blob/master/examples/seatbelts.csv)  
Output: [seatbelts_evaluate_output.csv](https://github.com/pemn/Namedtable/blob/master/examples/seatbelts_evaluate_output.csv)  
