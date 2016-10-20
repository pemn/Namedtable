#!perl
# example for Namedtables.pm


use strict;
use Namedtable;

my $table = new Namedtable('sales.csv');

my @statistics = (['Product'],['Quantity','sum']);

$table->breakdownToFile('sales_report_output', @statistics);
