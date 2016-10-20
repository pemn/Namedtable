#!perl
# example for Namedtables.pm
# using state x77 sample data


use strict;
use Namedtable;

my $table = new Namedtable('sales.csv');

my @statistics = (['Client Gender'],['Price(average product price)','average','Quantity']);

$table->breakdownToFile('sales_average_product_price_output', @statistics);
