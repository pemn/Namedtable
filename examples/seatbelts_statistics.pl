#!perl
# example for Namedtables.pm
# using UK seatbelts data


use strict;
use Namedtable;

my $table = new Namedtable('seatbelts.csv');

my @statistics = (['year','breakdown'],['law','breakdown'],['DriversKilled','sum'],['kms','sum'],['PetrolPrice','mean'],['DriversKilled(death by kms)','mean','kms']);

$table->breakdownToFile('seatbelts_statistics_output.csv', @statistics);
