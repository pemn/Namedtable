#!perl
# example for Namedtables.pm
# using state x77 sample data


use strict;
use Namedtable;

my $table = new Namedtable('state_x77.csv', {condition => 'Income > 5000'});

my @statistics = (['Population','sum'],['Income(Income by Population)','average','Population']);

$table->breakdownToFile('state_x77_condition_output.csv', @statistics);
