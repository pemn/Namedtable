#!perl
# example for Namedtables.pm
# using UK seatbelts data


use strict;
use Namedtable;

my $table = new Namedtable('seatbelts.csv');

$table->applyExpression('both_sides = front + rear');

$table->dump('seatbelts_evaluate_output.csv');
