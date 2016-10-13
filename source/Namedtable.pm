#!perl
#
# Copyright 2011-2016 Vale
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Namedtable v3.9
# class to serve as interface to a single Sheet, using simple set and get operations
# with column names that reference to a 2d perl array which may be directly tied to
# a input file using one of the available backends

package Namedtable;
use strict;
use List::Util qw/min max sum first/;
use Scalar::Util qw(looks_like_number);
use Cwd 'abs_path';

# create a object on this class
sub new {
    my ($class, $path, $opt) = @_;
    my $self = {};
    # synonyms allow for a column to have many alternate names
    # any of those names can be used to reference to the same column
    $self->{synonyms} = $opt->{synonyms};

    $self->{data} = [];
    $self->{type} = 'array';
    unless(ref($path)) {
        $self->{path} = $path;
    }
    bless $self;
    # parse a table into a array of hashes with column names
    # the table type is detected from extension
    # if file is invalid, returns empty data
    if($path eq '') {
        # do nothing
    } elsif(ref($path) eq 'ARRAY') {
        $self->{type} = 'array:native';
        # create a table from scratch by adding the header 
        $self->{data} = $path;
    # process vulcan block models
    } elsif(ref($path) eq 'vulcan::block_model' || $path =~ /\.bmf$/i) {
        $self->{type} = 'super:bm';
        $self->{super} = tie @{$self->{data}}, 'Tiebmf', $path, $opt;
        # since tiebmf already handled conditions, prevent the generic filterTable that is called later
        delete $opt->{condition};
    # process csv or space separated ascii
    } elsif($path =~ /\.(csv|txt|asc|prn)$/i) {
        # if(($ENV{PROCESSOR_ARCHITECTURE} ne 'AMD64' and -s $path > 1000000000) || ($ENV{PROCESSOR_ARCHITECTURE} eq 'AMD64' and -s $path > 0000000000)) {
        if($opt->{mode} eq 't') {
            $self->{type} = 'super:csv';
            # handle ascii csv files using Tie which allows large files to be loaded
            # without depleting adressable memory
            # if Package Tiecsv is in the same .pm file as this we dont need a "require" or "use"
            tie @{$self->{data}}, 'Tiecsv', $path;
        } else {
            $self->{type} = 'array:csv';
            # cache the entire csv in memory
            $self->csv2array($path);
        }
    # process vulcan databases
    } elsif($path =~ /(.+\.isis)!?(\w*)$/i) {
        $self->{type} = 'array:isis';
        if($2) {
            $opt->{table} = $2;
        }
        #$self->{super} = tie @{$self->{data}}, 'Tieisis', $1, $opt;
        $self->isisdb2array($path, $opt->{table});
    # process excel sheets
    # check for a sheet selector in the format !TABLE after the path
    } elsif($path =~ /(.+\.xls\w?)!?(\w*)$/i) {
        $self->{type} = 'array:excel';
        $self->xls2array($1, $2);
    # process vulcan mapfile
    } elsif($path =~ /\.dmp$/i) {
        $self->{type} = 'array:mapfile';
        $self->dmp2array($path);
    # process datamine table
    } elsif($path =~ /\.dm$/i) {
        $self->{type} = 'super:dm';
        # use tie to read big datamine files which otherwise would deplete memory
        $self->{super} = tie @{$self->{data}}, 'Tiedm', $path, $opt;
    } elsif($path =~ /\.(dat|out)$/i) {
        $self->{type} = 'array:dat';
        $self->dat2array($path);
    }
    
    # cache a table associating field name to field index
    $self->cache_header();
    
    # delete records that dont pass the condition evaluation
    if(exists $opt->{condition} && $opt->{condition}) {
        if($self->{type} =~ /dm$/) {
            $self->{super}->{id_lookup} = $self->filterExpression($opt->{condition});
            delete $opt->{condition};
        } else {
            $self->applyExpression($opt->{condition}, 1);
        }
    }
    return $self;
}

sub isVolatile {
    my ($self) = @_;
    for(qw/super:bm super:dm super:csv/) {
        return (0) if($self->{type} eq $_);
    }
    return(1);
}

# create or reset the column index cache
sub cache_header {
    my ($self) = @_;
    $self->{synonyms_cache} = {};
    $self->{header} = {};
    for(my $i=0;$i<=$#{$self->{data}[0]}; $i++) {
        my $name = $self->{data}[0][$i];
        # handle the case of empty column names and duplicate names
        if($name =~ /^\s*$/ || exists $self->{header}{$name}) {
            $name = $self->{data}[0][$i] = ( $name ? $name . "_dup" : "col_" ) . ($i+1);
        }
        $self->{header}{$name} = $i;
    }    
}

# remove all data rows, leaving only the header row
sub clear {
    my ($self) = @_;
    $#{$self->{data}} = 0;
}

#~ sub EXTEND { ... }
#~ sub DESTROY { ... }


# field separator detector
# input: one line of text delimited by something
# output: a character assumed to be the field separator
# v1.0 uses s///g instead of tr///
sub fsd {
    my ($l) = @_;
    my ($m,$fs) = (0,'');
    for(",",";","\t"," ") {
        my $n = $l =~ s/$_//g;
        if($n > $m) {
            $m = $n;
            $fs = $_;
        }
    }
    return($fs);
}

# parse a ascii CSV as a header hash and a data array
sub csv2array {
    my ($self, $path) = @_;
    my $data = $self->{data};
    local $,;
    open CSV2ARRAY, $path or die $!;
    my $blank_row = undef;
    while(<CSV2ARRAY>) {
        chomp;
        unless($,) {
            $, = fsd($_);
            # ensure the fs is one of the valid options: , ; tab
            if($, !~ /[,;\t]/) {
                $, = ',';
            }
        }
        # custom split by separator:
        # - preserve quoted strings
        # - handle empty first column
        # - handle empty columns (including last)
        my (@row) = ($_ =~ /"[^"]*"|[^$,]+|(?<=\A)(?=$,)|(?<=$,)(?=$,|\Z)/ig);
        # - trim leading and trailing spaces
        # - handle the special "" case, which may not be treated in the regexp 
        #   since a zero length match has lower priority to a sized match
        @row = map {s/^\s*"?|"?\s*$//g; $_} @row;
        # ignore blank rows
        if(scalar(@row) > 0) {
            push @$data, \@row;
        }
        # detect rows with only blank columns, but dont do anything about them yet
        if($_ =~ /^[$,]*$/) {
            $blank_row = $#$data;
        } else {
            $blank_row = undef;
        }
    }
    close CSV2ARRAY;
    # remove file trailing rows with only blank columns
    if(defined($blank_row)) {
        while($#$data >= $blank_row) {
            pop @$data;
        }
    }
}

# read a excel file as a header hash and a data array
# uses OLE as backend
sub xls2array {
    require Win32::OLE;
    my ($self, $path, $sheet_id) = @_;
    # convert relative path to absolute path
    my $Workbook = Win32::OLE->GetObject(abs_path($path));
    unless($Workbook) {
        print("Cant open $path");
        return;
    }
    my $Sheet = $Workbook->ActiveSheet;
    if($sheet_id) {
        $Sheet = $Workbook->Sheets($sheet_id);
    }
    my $data = $self->{data};
    #Const xlCellTypeLastCell = 11
    use constant xlCellTypeLastCell => 11;
    my $nrows = $Sheet->Cells->SpecialCells(xlCellTypeLastCell)->Row;
    my $ncols = $Sheet->Cells->SpecialCells(xlCellTypeLastCell)->Column;

    my $blank_row = undef;
    # detect which column belongs to which header
    for(my $r = 1;$r <= $nrows; $r++) {
        # need to throw a exception here in case excel is buzy or was closed
        my $region = $Sheet->Range($Sheet->Cells($r, 1), $Sheet->Cells($r, $ncols));
        my $line = $region->{Value};
        # handle the case where the region is a single cell and the returned value is not a array
        unless(ref($line)) {
            $line = [[$line]];
        }

        # fix for dates being returned as integers instead of strings
        for(my $i=0;$i<=$#{$line->[0]};$i++) {
            if(ref $line->[0][$i]) {
                $line->[0][$i] = $Sheet->Cells($r, $i + 1)->Win32::OLE::valof({'Value'});
            } else {
                # trim leading and trailing spaces from all cells
                $line->[0][$i] =~ s/^\s*|\s*$//g;
            }
            # if any column is not blank, reset the trailing empty row flag
            if(defined($blank_row) and $line->[0][$i]) {
                $blank_row = undef;
            }
        }
        push @$data, $line->[0];
        # trailing blank row flag, stores last VALID row
        if(not defined($blank_row)) {
            $blank_row = $#$data;
        }
    }
    # remove file trailing rows with only blank columns
    if(defined($blank_row)) {
        while($#$data > $blank_row) {
            pop @$data;
        }
    }
}

# read a vulcan database file
sub isisdb2array {
    my ($self, $path, $sheet_id) = @_;
    # we should isolate this on a package to remove vulcan dependency
    use vulcan;
    my $db = new vulcan::isisdb($path, 'r', '');
    unless(defined($sheet_id)) {
        ($sheet_id) = reverse($db->table_list());
    } else {
        $sheet_id = uc($sheet_id);
    }
    my $data = $self->{data};
    my @vl = $db->field_list($sheet_id);
    $data->[0] = ['KEY', @vl];
    for($db->rewind();! $db->eof;$db->next) {
        if($db->get_table_name() eq $sheet_id) {
            push @$data, [$db->get_key(), map {$db->get_string($_)} @vl];
        }
    }

    $db->close();
}

# adapted version of MapfileData, with return value compatible with xls2array and csv2array
sub dmp2array {
    my ($self, $path) = @_;
    my $data = $self->{data};
    my %p; # field position in row
    my $cursor = 0; # cursor that will be used to store positions in the header
    my @header; # ordered column names
    open DMP, $path or die $!;
    while(<DMP>) {
        chomp;
        # header
        if(substr($_,0,1) eq '*') {
            if(/^\*\s+(\w+)\s+(I|C|F)\s+(\d+)\s*(\d*)/) {
                my $name = $1;
                if(first {$_ eq $name} @header) {
                    $name .= '_' . scalar(@header);
                }
                # this encodes:
                # position as $p{x} % 65536
                # length as $p{x} / 65536 % 256
                # decimals as ($p{$t} / 16777216 % 256)-1.
                # the -1 is because 0 is a character field, and 0 decimals is stored as 1
                $p{$name} = $cursor + $3 * 65536 + ($2 eq 'C' ? 0 : ($4+1) * 16777216);
                $cursor += $3;
                push @header, $name;
            }
        } else {
            my @row;
            for my $t (@header) {
                my $v = substr($_,$p{$t} % 65536,$p{$t} / 65536 % 256);
                # trim spaces
                $v =~ s/^\s+|\s+$//g;
                push @row, $v;
            }
            push @$data, \@row;
        }
    }
    close DMP;
    unshift @$data, \@header;
}

# read a file in plurigaussian dat format
sub dat2array {
    my ($self, $path) = @_;
    my $data = $self->{data};
    if(open(INPUT_DAT, $path)) {
        my @header;
        my $n_col = -1;
        while(<INPUT_DAT>) {
            chomp;
            s/^\s+//;
            if($n_col == 0) {
                push @$data, [split(/\s+/, $_)];
            } elsif($. == 1) {
                # nothing do do
            } elsif($. == 2) {
                # column count
                $n_col = $_;
            } elsif($n_col > 0) {
                push @header, $_;
                $n_col--;
                if($n_col == 0) {
                    push @$data, \@header;
                }
            }
        }
        close(INPUT_DAT);
    }
    return();
}

# generate a file in the plurigaussian dat format
sub dumpdat {
    my ($data, $path) = @_;
    local $, = " ";
    local $\ = "\n";

    if(open(OUTPUT_DAT, '>', $path)) {
        # craft the header
        # for($database, scalar(@vl), @vl) {
        print OUTPUT_DAT $path;
        for(my $i=0;$i<=$#$data;$i++) {
            my $row = $data->[$i];
            if($i == 0) {
                print OUTPUT_DAT scalar(@$row);
                for(@$row) {
                    print OUTPUT_DAT $_;
                }
            } else {
                print OUTPUT_DAT @$row;
            }
        }

        close(OUTPUT_DAT);
    }
}

# return the number of data rows in the given table
sub size {
    my ($self) = @_;
    # since the first row is the header, the effective count is equal to the last row of data table
    return($#{$self->{data}});
}

# alias for size()
sub getRowCount {&size}

# return the column index for a given column name
# this sub is the angular stone of Namedtables
sub getColumnIndex {
    my ($self, $col) = @_;
    my $synonym = $self->getSynonym($col);
    if(defined($synonym)) {
        # return the index of the column that is a synonym to the requested column
        return($self->{header}{$synonym});
    }
    return(-1);
}

# return the true name of a column in the header
sub getSynonym {
    my ($self,$col) = @_;
    # handle isis table:field syntax
    if($self->{type} eq 'array:isis') {
        if($col =~ /\w+\:(\w+)/) {
            $col = $1;
        }
    }

    # first search for a direct match
    if(exists $self->{header}{$col}) {
        return($col);
    }
    # now try to find a column that does not match the case
    unless(exists $self->{synonyms_cache}{$col}) {
        for(@{$self->{data}[0]}) {
            if(lc($_) eq lc($col)) {
                $self->{synonyms_cache}{$col} = $_;
            }
        }
    }

    # search the cache for the synonym
    # synonyms are always store as lower case
    unless(exists $self->{synonyms_cache}{$col}) {
        # then search on all synonyms
        for(my $i=0;$i<=$#{$self->{synonyms}};$i++) {
            my ($s, $c) = (-1, undef);
            for(my $j=0;$j<=$#{$self->{synonyms}[$i]};$j++) {
                # search for a column that matches the input name
                if($s == -1 && lc($col) eq lc($self->{synonyms}[$i][$j])) {
                    $s = $j;
                }
                # search for a column that actualy exists in the table
                unless(defined($c)) {
                    for(@{$self->{data}[0]}) {
                        if(lc($_) eq lc($self->{synonyms}[$i][$j])) {
                            $c = $_;
                            last;
                        }
                    }
                }
            }
            # we found both the synonym and a column that exists on the database
            if($s >= 0 && defined($c)) {
                $self->{synonyms_cache}{$col} = $c;
                last;
            }
        }
    }
   
    return($self->{synonyms_cache}{$col});
}

# create a synonym between any two names
# all synonyms are linked, so its possible to even create a synonym between two
# names that do not exist on the header and then link one of those names to a real name
# returns the column index of the column
sub createSynonym {
    my ($self,$name1,$name2) = @_;
    for(my $i=0;$i<=$#{$self->{synonyms}};$i++) {
        my ($s, $c) = (-1, -1);
        for(my $j=0;$j<=$#{$self->{synonyms}[$i]};$j++) {
            # search for a column that matches the input name
            if($s == -1 && $name1 eq $self->{synonyms}[$i][$j]) {
                $s = $j;
            }
            # search for a column that actualy exists in the table
            if($c == -1 && $name2 eq $self->{synonyms}[$i][$j]) {
                $c = $j;
            }
        }
        if($s >= 0 && $c == -1) {
            push @{$self->{synonyms}[$i]}, $name1;
            return($i);
        }
        if($c >= 0 && $s == -1) {
            push @{$self->{synonyms}[$i]}, $name2;
            return($i);
        }
    }
    for(keys(%{$self->{header}})) {
        if($name1 eq $_) {
            push @{$self->{synonyms}}, [$_, $name2];
            return($self->{header}{$_});
        }
        if($name2 eq $_) {
            push @{$self->{synonyms}}, [$_, $name1];
            return($self->{header}{$_});
        }
    }
    return(-1);
}

# checks if all given columns already exist in the table
# returns the first column that does not exist
sub checkColumns {
    my ($self, @vl) = @_;
    for(@vl) {
        if($self->getColumnIndex($_) == -1) {
            return($_);
        }
    }
    return();
}

# checks if all given columns already exist in the table
# returns a the list of columns that do exists
sub grepColumns {
    my ($self, @vl) = @_;
    for(my $i=$#vl;$i>=0;$i--) {
        if($self->getColumnIndex($vl[$i]) == -1) {
            # remove the inexisting variable
            splice(@vl, $i, 1);
        } else {
            $vl[$i] = $self->getSynonym($vl[$i]);
        }
    }
    return(@vl);
}

# return the data in a row for a list of columns
# input: row index (zero based, not counting header), column name
sub get {
    my ($self,$row,@vl) = @_;
    # if no row specified, return the complete table as a 2d array
    # but ommit the first row since its the header
    if(not defined($row)) {
        return(@{$self->{data}}[1 .. $#{$self->{data}}]);
    } 
    unless(@vl) {
        # return the complete row, adjusting the row index to not count the header
        return(@{$self->{data}[$row + 1]});
    }
    my @r;
    
    # hack that accesses the super object using a optimized FETCH
    if($self->{type} =~ /super/) {
        # convert synonyms to its real column names
        @r = @{$self->{super}->FETCH($row + 1, [map {$self->getSynonym($_)} @vl])};
    } else {
        # for each argument, retrieve a column with that name
        for(@vl) {
            my $n = $self->getColumnIndex($_);
            if($n != -1) {
                push @r, $self->{data}[$row + 1][$n];
            } else {
                push @r, undef;
            }
        }
    }
    # when we have only one variable as argument, return a scalar instead of array
    if(wantarray) {
        return(@r);
    } else {
        return($r[0]);
    }
}

# return the values of a list of column indexes
sub getByIndex {
    my $self = shift;
    my $row = shift;
    return(@{$self->{data}[$row + 1]}[@_]);
}

# retrive a list of columns on a given table
# optionaly filters any columns given as additional parameters
sub getFieldList {
    my ($self, @filter) = @_;
    # sort the columns by the order they appear on the input file
    my @columns = sort {$self->{header}{$a} <=> $self->{header}{$b}} keys %{$self->{header}};
    # convert filter names to indexes
    my %unique;
    for(my $i=0;$i<=$#filter;$i++) {
        my $index = $self->getColumnIndex($filter[$i]);
        if($index >= 0) {
            # flag this index for deletion
            $unique{$index} = undef;
        }
    }

    # remove the columns on the filter list from the output list
    for(my $i=$#columns;$i>=0;$i--) {
        # check if this column is marked for deletion
        if(exists $unique{$i}) {
            splice(@columns, $i, 1);
        }
    }
    return(@columns);
}

# alias for getFieldList()
sub getColumns {&getFieldList}

# for each field in this table, return 0 if its numeric type, 1 otherwise
sub getFieldTypes {
    my ($self) = @_;
    my @r;
    my $n = $self->size() - 1;
    # check a random sampling of rows to detect values
    for my $i (map {int(0.5 + $_ * $n * 0.01)} 0 .. 100) {
        my @row = $self->get($i);
        for(my $j=0;$j<=$#row;$j++) {
            # numeric check
            unless(looks_like_number($row[$j])) {
                $r[$j] = 1;
            }
        }
    }
    return(@r);
}

# alias for getFieldTypes
sub getColumnTypes {&getFieldTypes}

# return the number of data rows a given holeid has
sub getHoleRowCount {
    my ($self, $holeid) = @_;
    my $c = 0;

    my $col = $self->getTableKey();
    for(my $i=0;$i<$self->size();$i++) {
        if($self->get($i, $col) =~ /^\s*\Q$holeid\E\s*$/i) {
            $c++;
        }
    }
    return($c);
}

# return another column value in the rows that matche the given column value
sub lookup_data {
    my ($self, $col, $val, $datacol) = @_;
    my @r;
    for($self->lookup($col,$val)) {
        push @r, $self->get($_, $datacol);
    }
    return(@r);
}

# return the rows that matches the given column value
sub lookup {
    my ($self, $col, $val) = @_;
    my @ci;
    # input can be either a list or a scalar
    if(ref($col)) {
        for(@$col) {
            push @ci, $self->getColumnIndex($_);
        }
    } else {
        push @ci, $self->getColumnIndex($col);
        $val = [$val];
    }
    my @r;
    for(my $i=0;$i<$self->size();$i++) {
        my $hit = 1;
        for(my $j=0;$j<=$#ci;$j++) {
            if($self->getByIndex($i, $ci[$j]) ne $val->[$j]) {
                $hit = 0;
            }
        }
        if($hit) {
            push @r, $i;
        }
    }
    return(@r);
}

# returns a single data row for a given holeid, row index and column
# DOES NOT WORK if the holeid column is not in synonyms nor is the first column
sub getHoleRow {
    my ($self, $holeid, $row, @vl) = @_;

    my $col = $self->getTableKey();
    my $c = 0;
    my $r = -1;
    for(my $i=0;$i<$self->size();$i++) {
        if($self->get($i, $col) =~ /^\s*\Q$holeid\E\s*$/i) {
            # store the last matched row
            $r = $i;
            # stop when we hit the target row
            if($c == $row) {
                last;
            }
            $c++;
        }
    }
    if($r >= 0 && ($row == -1 || $row == $c)) {
        return($self->get($r, @vl));
    } else {
        return();
    }
}

# return a list of rows indexes that belong to a given holeid
sub getHoleRows {
    my ($self, $holeid, $key) = @_;
    my @r;
    unless(defined($key)) {
        $key = $self->getTableKey();
    }
    # cache the key column index
    my $n = $self->getColumnIndex($key);
    for(my $i=0;$i<$self->size();$i++) {
        # by using the column index instead of column name we increase performance
        if($self->getByIndex($i, $n) =~ /^\s*\Q$holeid\E\s*$/i) {
            push @r, $i;
        }
    }
    return(@r);
}

# return the list of unique values for a given table and column
# in the case the column is not specified, assume is the id columns
sub getUniqueValues {
    my ($self, $col) = @_;
    #~ $col = lc($col);

    unless($col && $self->getColumnIndex($col) != -1) {
        $col = $self->getTableKey();
    }
    
    my @r;
    for(my $i=0;$i<$self->size();$i++) {
        my ($v) = $self->get($i, $col);
        # skip blank values
        if($v =~ /\S+/) {
            # check if this value already exists in the output array
            for(@r) {
                if($v eq $_) {
                    $v = undef;
                }
            }
            # if value does not exists already, add it to output array;
            if($v ne undef) {
                push @r, $v;
            }
        }
    }
    return(@r);
}

# return the key field of this table
sub getTableKey {
    my ($self) = @_;
    my $col = 'holeid';
    unless(exists $self->{header}{$col}) {
        # get the first column of the table
        ($col) = $self->getFieldList();
    }
    return($col);
}

# return true if the given row is a blank line
sub isEmptyRow {
    my ($self, $i) = @_;
    if($self->getByIndex($i, 0) eq '') {
        return(1);
    }
    return(0);
}


# set a single value in a given row and column name
# input: table name, row index, column name
sub set {
    # shift the first 3 parameters
    my ($self, $row, $col, $val) = @_;
    return() if($row >= $self->size());
    # retrieve column index for the given name
    my $n = $self->getColumnIndex($col);
    # print 'set',$row,$col,$val,$n;
    # create a new column if it does not exist
    if($n == -1) {
        $n = $self->addColumn($col);
    }
    # if row was specified, set the value in a single row
    if(defined($row)) {
        # use this convoluted syntax to ensure compatibility with tied arrays
        my $rowdata = $self->{data}[$row + 1];
        $rowdata->[$n] = $val;
        $self->{data}[$row + 1] = $rowdata;
        
    } else {
        # if no row was specified, set the value on all rows
        for(my $i=0;$i<$self->size();$i++) {
            # use this convoluted syntax to ensure compatibility with tied arrays
            my $rowdata = $self->{data}[$i + 1];
            $rowdata->[$n] = $val;
            $self->{data}[$i + 1] = $rowdata;
        }
    }
}

sub setRow {
    my ($self, $row, $rowdata) = @_;
    $self->{data}[$row + 1] = $rowdata;
}

# evaluates code interpolating columns names with their value on the given row
# returns the eval value and the interpolated string
# if the strict values is true, will treat missing columns as 0 instead of returning original value
sub evaluateExpression {
    my ($self, $row, $expression, $strict) = @_;

    # check the assignment buffer for update values
    my %update;
    for($expression =~ /[A-Za-z][\w\:]+(?=\s+[\+\-\*\/\.]?=~?\s+)/mg) {
        $update{$_} = undef;
    }
    my %tokens;
    my %s;
    # parse the input tokens
    # tokens are sequences after a space or the beggining of the string, that start with a alpha characters
    for($expression =~ /[A-Za-z][\w\:]+/mg) {
        next if (exists $tokens{$_});
        # retrieve the index of the column name, -1 if that column does not exist
        my $n = $self->getColumnIndex($_);
        # for the tokens that match a column, store the value of that column
        # otherwise keep the original value as both key and value
        if($n != -1) {
            my $v = $self->getByIndex($row, $n);
            if($v == -99) {
                # convert -99 values to undef
                $v = undef;
            }
            $s{$_} = $v;
            $tokens{$_} = "\$s{'$_'}";
        } elsif($_ eq 'ROW') {
            # magic variable that gives the current row
            $tokens{$_} = $row + 1;
        } elsif($strict) {
            # if we are in strict mode, unknown tokens will default to 0
            $tokens{$_} = 0;
        } else {
            # dont change unknown token
            $tokens{$_} = $_;
        }
    }
    
    # replace the tokens with the handing expression
    $expression =~ s/[A-Za-z][\w\:]+/$tokens{$&}/g;
    # evaluate the baked expression
    my ($r) = eval($expression);
    # if a error ocurred, print debug info
    if ( $@ ) {
        print("error evaluating row $row:\n$@\n"); # $expression\n
    }
    #print("$expression\n");
    for(keys %update) {
        $self->set($row, $_, $s{$_});
        # in case we have ANY assignment, ensure this function will return true
        $r = 1;
    }
    # check if we were called in list context for additional info or scalar context for just the status
    if(wantarray) {
        return($r,$expression);
    } else {
        return($r);
    }
}

# create a left join of two tables by key and by intersecting from->to intervals
sub intersectIntervals {
    my ($self, $other, $key, @vl) = @_;
    unless($key) {
        $key = 'holeid';
    }
    # this columns will be the first on the output file
    my @hardcols = ($key, 'from', 'to', 'length');
    my @softcols2 = $other->getFieldList(@hardcols);
    # the second (other) table will have priority on the columns
    # this addresses the common case where we want to update columns that already exist on the base table
    my @softcols1 = $self->getFieldList(@hardcols, @softcols2);
    my %weight;
    # if variables were specified, enable variable ponderation
    # otherwise, all variables are simply duplicated on every break
    if(@vl) {
        for(my $i=$#softcols1;$i>=0;$i--) {
            for(@vl) {
                if($softcols1[$i] eq $_->[0]) {
                    $weight{$_->[0]} = $_->[1];
                }
            }
        }
        for(my $i=$#softcols2;$i>=0;$i--) {
            for(@vl) {
                if($softcols2[$i] eq $_->[0]) {
                    $weight{$_->[0]} = $_->[1];
                }
            }
        }
    }
    
    my @r = [@hardcols, @softcols1, @softcols2];
    for my $holeid1 ($self->getUniqueValues($key)) {
        my @rows1 = $self->getHoleRows($holeid1, $key);
        my @rows2 = $other->getHoleRows($holeid1, $key);
        # check if the second table contains rows for this holeid
        if(@rows2) {
            my ($from2, $to2, $i2);
            my ($from1, $to1);
            # create a raw list of intersected intervals
            for(intersect_intervals_1d([map {[$self->get($_, qw/from to/),$_]} @rows1],[map {[$other->get($_, qw/from to/),$_]} @rows2])) {
                # retrieve the from, to and row indexes in each of the two tables for this intersection
                my ($from, $to, $i1, $i2) = @$_;
                my $len = $to - $from;
                my @data1 = (defined($i1) ? $self->get($i1, @softcols1) : (undef) x scalar(@softcols1));
                my @data2 = (defined($i2) ? $other->get($i2, @softcols2) : ());
                # check if we are operating in ponderation mode
                if(@vl) {
                    # if this field is ponderated by sum, weight by interval length
                    for(my $i=0;$i<=$#softcols1;$i++) {
                        if($weight{$softcols1[$i]} eq 'sum') {
                            my ($original_from, $original_to) = $self->get($i1, 'from', 'to');
                            my $original_length = $original_to - $original_from;
                            if($original_length > 0) {
                                $data1[$i] = (defined($data1[$i]) ? $data1[$i] * min(1, $len / $original_length) : undef);
                            }
                        }
                    }
                    for(my $i=0;$i<=$#softcols2;$i++) {
                        if($weight{$softcols2[$i]} eq 'sum') {
                            my ($original_from, $original_to) = $other->get($i2, 'from', 'to');
                            my $original_length = $original_to - $original_from;
                            if($original_length > 0) {
                                $data2[$i] = (defined($data2[$i]) ? $data2[$i] * min(1, $len / $original_length) : undef);
                            }
                        }
                    }
                }
                push @r, [$holeid1, $from, $to, $len, @data1, @data2];
            }
        } else {
            for(@rows1) {
                push @r, [$self->get($_, @hardcols, @softcols1)];
            }
        }
    }
    return(@r);
}

# specialized function to intersect intervals
# returns a list with the first two columns containing the from and to 
# plus a column for each set, containing the value of that set in that interval (or undef)
sub intersect_intervals_1d {
    my @break;
    my @oset;
    for my $set (@_) {
        # consolide all FROM and TO values in a single list
        uniqueset_add(\@break, map {$_->[0]} @$set);
        uniqueset_add(\@break, map {$_->[1]} @$set);
        # sort so smaller intervals are found first
        # this is to fix the case where big intervals generated by gaps override real intervals
        push @oset, [sort {abs($a->[0] - $a->[1]) <=> abs($b->[0] - $b->[1])} @$set];
    }
    # order the combined FROM and TO so we have every possible break point
    @break = sort {$a <=> $b} @break;
    my @r;
    # create the final result, a list of intervals and the corresponding value for each set
    for(my $i=1;$i<=$#break;$i++) {
        my ($from, $to) = ($break[$i-1], $break[$i]);
        push @r, [$from, $to, map {find_containing_interval($from, $to, $_)} @oset];
    }
    return(@r);
}

# do a interval lookup in a table with 3 columns: from,to,value
sub find_containing_interval {
    my ($from, $to, $set) = @_;
    my $c;
    # sort so smaller intervals are found first
    # this is to fix the case where big intervals generated by gaps override real intervals
    # my @oset = sort {abs($a->[0] - $a->[1]) <=> abs($b->[0] - $b->[1])} @$set;
    for(my $j=0;$j<=$#$set;$j++) {
        if($from >= $set->[$j][0] && $to <= $set->[$j][1]) {
            $c = $set->[$j][2];
            last;
        }
    }
    return($c);
}

# add a value to a set of unique values (should be more efficient that a hash)
sub uniqueset_add {
    my $set = shift;
    for(@_) {
        my $i=$#$set;
        # check if this value already exist on the set
        while($i>=0) {
            last if($set->[$i] == $_);
            $i--;
        }
        # if values was not found, add it to the set
        if($i == -1) {
            push @$set, $_;
        }
    }
}

# create a join of two tables by a primary key
# supported operations: left,right,inner,outer
# key: the primary key. if empty uses row number
# v1.2 2015/09 paulo.ernesto
sub join {
    my ($self, $other, $key, $type, @usercols) = @_;
    my %table1ids;
    my %table2ids;
    # cache the rows on the second table that belong to each hole id
    for(my $i=0;$i<$other->size();$i++) {
        my ($id) = ($key ? $other->get($i, $key) : $i);
        $table2ids{$id} = $i;
    }

    my (@softcols1, @softcols2);

    if($type =~ /right/i) {
        # in the case columns exists on both tables, use those from table2
        @softcols2 = $other->getFieldList($key);
        if(@usercols) {
            @softcols1 = @usercols;
        } else {
            # get columns from table1
            @softcols1 = $self->getFieldList($key, @softcols2);
        }
    } else {
        # in the case columns exists on both tables, use those from table2
        @softcols1 = $self->getFieldList($key);
        if(@usercols) {
            @softcols2 = @usercols;
        } else {
            # get columns from table1
            @softcols2 = $other->getFieldList($key, @softcols1);
        }
    }


    my @r = ([$key ? $key : (), @softcols1, @softcols2]);
    
    for(my $i=0;$i<$self->size();$i++) {
        my ($id) = ($key ? $self->get($i, $key) : $i);
        $table1ids{$id} = $i;
        if(exists $table2ids{$id}) {
            # those rows are contained in: inner,left,right,outer
            push @r, [$key ? $id : (), (@softcols1 ? $self->get($i, @softcols1) : ()), (@softcols2 ? $other->get($table2ids{$id}, @softcols2) : ())];
        } elsif($type =~ /outer|left/i) {
            # those rows are contained in: left,outer
            push @r, [$key ? $id : (),(@softcols1 || @softcols2 ? $self->get($i, @softcols1, @softcols2) : ())];
        }
    }
    # the right is in fact a outter right
    if($type =~ /outer|right/i) {
        for(my $i=0;$i<$other->size();$i++) {
            my ($id) = ($key ? $other->get($i, $key) : $i);
            unless(exists $table1ids{$id}) {
                # those rows are contained in: right,outer
                push @r, [$key ? $id : (),$other->get($i, @softcols1, @softcols2)];
            }
        }
    }
    return(@r);
}

# static function to append multiple tables into a single output csv
# concatenate multiple tables
# header is a join of all columns found in all tables
sub append {
    my (@tables) = @_;
    
    # create the common header
    my %header;
    for my $table (@tables) {
        for my $col (keys %{$table->{header}}) {
            $header{$col} = $table->{header}{$col};
        }
    }
    
    # sort the combined header by column index
    my @header = sort {$header{$a} <=> $header{$b}} keys %header;

    my $output = new Namedtable([\@header]);
    for my $table (@tables) {
        for(my $i=0;$i<$table->size();$i++) {
            $output->addRow([$table->get($i, @header)]);
        }
    }
    # check if we want raw data instead of a object
    if(wantarray) {
        return($output->get());
    }
    return($output);
}

# return a list of rows where a given column has a specific value
sub getKeyRows {
    my ($self, $col, @val) = @_;
    my @r;
    for(my $i=0;$i<$self->size();$i++) {
        my $hit = 1;
        for(@val) {
            if($self->get($i, $col) ne $_) {
                $hit = 0;
            }
        }
        if($hit) {
            push @r, $i;
        }
    }
    return(@r);
}

# save a table to a csv file or to STDOUT
sub dump {
    my ($self, $path) = @_;

    # raw data in a perl array
    if(wantarray) {
        # almost like a get(), but with header line
        return(@{$self->{data}});
    }
    dump_path($self->{data}, $path);
}

sub dump_path {
    my ($data, $path) = @_;

    if(ref($data) ne 'ARRAY') {
        print STDERR ("dump_path: invalid data\n");
        return();
    }
    
    if($path =~ /\.xls.?$/i) {
        # excel format
        &excel_tables;
    } elsif($path =~ /\.(dat|out)$/i) {
        # handle the custom plurigaussian format
        &dumpdat;
    } else {
        # csv format (default)
        &dumpcsv;
    }
}

sub dumpcsv {
    my ($data, $path) = @_;
    local $, = ",";
    local $\ = "\n";

    # tab separated formats
    if($path =~ /\.(?:prn|asc|txt)$/i) {
        $, = "\t";
    }

    # ensure the csv extension on files without extension
    if(defined $path && $path && $path !~ /\.\w+$/) {
        $path .= '.csv';
    }
    my $out;
    # create OUT as a pipe to a file
    if((not defined $path) or (not $path)) {
        # if we cant create the file, create DUMP as a dup to STDOUT
        open($out, ">&STDOUT") or return($!);
    } elsif($path =~ /^(.*?)(\w+)(\.g?zip)$/i) {
        require Compress::Zlib;
        if(lc($2) ne 'csv') {
            $path = $1 . $2 . '.csv' . $3;
        }
    
        $out = Compress::Zlib::gzopen($path, 'w');
        for(my $i=0;$i<=$#$data;$i++) {
            $out->gzwrite(join($, , map {$_ =~ /^[^"]*,[^"]*$/ ? '"' . $_ . '"' : $_} @{$data->[$i]}) . $\);
            # quote fields that contain "," and were not already quoted
        }
        $out->gzclose();
        return();
    } else {
        open($out, '>', $path) or return($!);
    }
    
    for(my $i=0;$i<=$#$data;$i++) {
        print $out map {$_ =~ /^[^"]*,[^"]*$/ ? '"' . $_ . '"' : $_} @{$data->[$i]};
        # quote fields that contain "," and were not already quoted
    }

    close($out);
}

# save a table as a vulcan dmp file
sub dumpdmp {
    my ($self, $path) = @_;
    if(open DUMP, '>', $path) {
        local $, = ",";
        local $\ = "\n";
        my $n_variables = scalar(keys %{$self->{header}});
        printf DUMP ("*\n*\n");
        printf DUMP ("* VARIABLES            %d\n", $n_variables);
        my @type = ('F') x $n_variables;
        my @size = (20) x $n_variables;
        for(my $i=0;$i< $self->size();$i++) {
            for(my $j=0;$j< $n_variables;$j++) {
                my $v = $self->getByIndex($i, $j);
                unless(looks_like_number($v)) {
                    $type[$j] = 'C';
                }
                if(length($v) > $size[$j]) {
                    $size[$j] = length($v);
                }
            }
        }
        printf DUMP ("*\n*\n");
        for(sort {$self->{header}{$a} <=> $self->{header}{$b}} keys %{$self->{header}}) {
            if($type[$self->{header}{$_}] eq 'C') {
                printf DUMP ("* %-20s C %s\n", $_, $size[$self->{header}{$_}]);
            } else {
                printf DUMP ("* %-20s F %s 4\n", $_, $size[$self->{header}{$_}]);
            }
        }
        printf DUMP ("*\n*\n");
        for(my $i=0;$i< $self->size();$i++) {
            print DUMP pack("(A20)*", $self->get($i));
        }
        close DUMP ;
    }
}

# create a empty copy of this table
sub clone {
    my ($self) = @_;
    my $clone = new Namedtable(undef, $self);
    # create a deep copy of the header
    for(keys %{$self->{header}}) {
        $clone->{header}{$_} = $self->{header}{$_};
    }
    if(scalar(@{$self->{data}[0]}) > 0) {
        $clone->{data}[0] = [@{$self->{data}[0]}];
    }
    return($clone);
}

# add a new column if it does not exists already and return the index
sub addColumn {
    my ($self, $col) = @_;
    # check if column exists
    my $r = $self->getColumnIndex($col);
    if($r == -1) {
        my $buffer = $self->{data}[0];
        $r = 0;
        # find the first non-blank column
        for(my $i=$#$buffer;$i>0;$i--) {
            if($buffer->[$i]) {
                $r = $i + 1;
                last;
            }
        }
        # add a new column
        $self->{header}{$col} = $r;
        # add the column to the first data line (header)
        # remember it may be a tied array so leave the convoluted code as it is
        $buffer->[$r] = $col;
        $self->{data}[0] = $buffer;
    }
    # return the index of the existing or of the new column
    return($r);
}

# rename a existing column or add a new column
sub renameColumn {
    my ($self, $old_name, $new_name) = @_;
    # check if column exists
    my $r = $self->getColumnIndex($old_name);
    if($r < 0) {
        $self->addColumn($new_name);
    } else {
        delete $self->{header}{$old_name};
        $self->{header}{$new_name} = $r;
        # rename the column in the first row
        # remember it may be a tied array so lead the convoluted code as it is
        my $buffer = $self->{data}[0];
        $buffer->[$r] = $new_name;
        $self->{data}[0] = $buffer;
    }
    # return the index of the existing or of the new column
    return($r);
}
# rename a existing column or add a new column
sub deleteColumn {
    my ($self, $col) = @_;
    # check if column exists
    my $r = $self->getColumnIndex($col);
    if($r != -1) {
        delete $self->{header}{$col};
        # remember it may be a tied array so lead the convoluted code as it is
        for(my $i=0;$i<=$#{$self->{data}};$i++) {
            my $buffer = $self->{data}[$i];
            splice(@$buffer, $r, 1);
            $self->{data}[$i] = $buffer;
        }
        # reset all column index caches since they may now point to invalid indexes
        $self->cache_header();
    }
}

# append a row to the end of data array
sub addRow {
    my ($self, $data, $index) = @_;
    # if we have a index, insert row at position
    if(defined($index)) {
        splice(@{$self->{data}}, $index + 1, 0, $data);
    } else {
        # append row
        push @{$self->{data}}, $data;
    }
}

# delete the row with the given index from the data array
# return the deleted row
sub delRow {
    my ($self, $row) = @_;
    return(splice(@{$self->{data}}, $row + 1, 1));
}

# run a expression on the entire table
# may be used to add values or filter the table
sub applyExpression {
    my ($self, $expression, $delete) = @_;
    # detect new column name from expression
    for($expression =~ /\s*(\w+)\s*=/g) {
        if($self->getColumnIndex($_) == -1) {
            $self->addColumn($_);
        }
    }

    # delete table rows where the expression does not return true
    for(my $i=$self->size()-1;$i>=0;$i--) {
        if(!$self->evaluateExpression($i, $expression) && $delete) {
            $self->delRow($i);
        }
    }
}

# return a list of rows that match the given expression
sub filterExpression {
    my ($self, $expression) = @_;
    my @r;
    for(my $i=0;$i<$self->size();$i++) {
        if($self->evaluateExpression($i, $expression)) {
            push @r, $i;
        }
    }
    return(\@r);
}

# remove empty rows at the end of the table
sub trimRows {
    my ($self) = @_;
    my $r = $self->{data};
    for(my $i=$#$r;$i>=1;$i--) {
        my $blank = 1;
        for(my $j=0;$j<=$#{$r->[$i]};$j++) {
            if($r->[$i][$j]) {
                $blank = 0;
                last;
            }
        }
        if($blank) {
            splice(@$r, $i, 1);
        }
    }
}

# sort the table by the given columns
# use a swatzian transform to avoid hitting the maximum addressable memory limit
sub sortByColumns {
    my ($self) = shift;
    # cache column indexes to improve efficiency
    my @col_index = map {$self->getColumnIndex($_)} @_;
    # sort by the given columns
    my @buffer = sort {my $r; for(@col_index) {$r = $r || ($a->[$_] =~ /^[Ee\d\.\+\-\s]+$/ ? $a->[$_] <=> $b->[$_] : $a->[$_] cmp $b->[$_])} $r} $self->get();
    
    for(my $i=0;$i<=$#buffer;$i++) {
        $self->{data}[$i + 1] = $buffer[$i];
    }

    return();
}

# compare rows and columns between two tables
# returns the rows which only exists on one of those tables
sub compare {
    my ($self, $other, @vl) = @_;
    my @buffer;
    for(my $i=0;$i< $self->size();$i++) {
        push @buffer, [$self->get($i, @vl),$self->{path},$i];
    }
    for(my $i=0;$i< $other->size();$i++) {
        push @buffer, [$other->get($i, @vl),$other->{path},$i];
    }
    # sort by the vl columns
    @buffer = sort {my $r; for (0 .. $#vl) {$r = $r || $a->[$_] cmp $b->[$_]}; $r} (@buffer);
    
    # for(@buffer) {
        # print "@$_\n";
    # }
    # eliminate rows which exist on both files
    # they appear as sequential duplicates on the ordered list
    for(my $i=$#buffer-1;$i>=0;$i--) {
        my $diff = undef;
        for(my $j=0;$j<=$#vl;$j++) {
            if($buffer[$i][$j] ne $buffer[$i+1][$j]) {
                # numeric check and comparison
                unless(looks_like_number($buffer[$i][$j]) && looks_like_number($buffer[$i+1][$j]) && abs($buffer[$i][$j] - $buffer[$i+1][$j]) < 0.01) {
                   $diff = $vl[$j];
                   last;
                }
            }
        }
        if(not defined($diff)) {
            splice(@buffer, $i, 2);
            $i--;
        } else {
            push @{$buffer[$i]}, $diff;
        }
    }
    unshift @buffer, [@vl,'file','row','diff'];
    return(@buffer);
}

# breakdown v7.3
# class to accumulate and report values broken by one or more breakdown variables
sub breakdown {
    my ($self, @vl) = @_;
    $self->{__breakdown_tree} = {};
    $self->{__breakdown_count} = -1;
    my $getid = "getId";
    # variable ponderation types:
    # breakdown (default, group all values with this key)
    # group (same as breakdown, but break when the groups are not contiguous)
    # major
    # min
    # max
    # average[,weight]...
    # sum[,weight]...
    # count
    # list
    # list_weight
    # output buffer, may stay empty if we are printing to STDOUT
    my @output;
    # list of columns indexes with the type breakdown
    my @breakdown;
    # run a few checks on the variable names and types
    for(my $i=$#vl;$i>=0;$i--) {
        # clone the input array so we dont modify input data
        $vl[$i] = [@{$vl[$i]}];
        # remove alternate column names and any other garbage from column names
        if($vl[$i][0] =~ /^(.*)\(.+\)/) {
            $vl[$i][0] = $1;
        }
        # the columns with blank type or with the "breakdown" type will be the breakdown columns
        unless($vl[$i][1]) {
            $vl[$i][1] = 'breakdown';
        }
        if($vl[$i][1] =~ /(breakdown|group)/i) {
            if(lc($1) eq 'group') {
                $getid = "getIdGroup";
            }
            # store the table index of this breakdown column on the breakdown cache
            unshift @breakdown, $vl[$i][0];
        }
        # check if weights for this column exists on the table
        for(my $k=2;$k <= $#{$vl[$i]};$k++) {
            # numeric check, then multiply by this number
            if(looks_like_number($vl[$i][$k])) {
                # do nothing
            } elsif((not defined($vl[$i][$k])) or $vl[$i][$k] eq '') {
                $vl[$i][$k] = undef;
            } elsif($self->getColumnIndex($vl[$i][$k]) == -1) {
                $vl[$i][$k] = undef;
            }
        }
    }
    # create all breakdown indexes
    my @breakrows;
    for(my $i=0;$i<$self->size();$i++) {
        # special case: we may not have any breakdown fields
        my $j = (scalar(@breakdown) > 0 ? $self->$getid([$self->get($i, @breakdown)]) : 0);
        push @{$breakrows[$j]}, $i;
    }
    my %postprocess;
    # now for each indexed group of rows, create the agregated row
    for(my $j=0;$j<=$#breakrows;$j++) {
        # the counters are scoped to this breakdown block
        my (@row, @row_aux);
        # interact with each row of this group
        for my $r (@{$breakrows[$j]}) {
            for(my $i=0;$i<=$#vl;$i++) {
                my $type = lc($vl[$i][1]);
                
                # constant string
                if ($type eq 'text') { 
                    $row[$i] = $vl[$i][0];
                    next;
                }
                my $value = $self->get($r, $vl[$i][0]);
                # ignore blank values 
                next if ((not defined($value)) or $value == -99);
                
                # switch between the possible types:
                if($type eq 'breakdown') { # every unique value will create a new row
                    $row[$i] = $value;
                } elsif($type eq 'mean' or $type eq 'average') { # weighted average
                    # calculate the mean using a online algorithm:
                    #def weighted_incremental_variance(dataWeightPairs):
                    #    sumweight = 0
                    #    mean = 0
                    #    M2 = 0
                    # 
                    #    for x, weight in dataWeightPairs:  # Alternatively "for x, weight in zip(data, weights):"
                    #        temp = weight + sumweight
                    #        delta = x - mean
                    #        R = delta * weight / temp
                    #        mean = mean + R
                    #        M2 = M2 + sumweight * delta * R  # Alternatively, "M2 = M2 + weight * delta * (x-mean)"
                    #        sumweight = temp
                    # 
                    #    variance_n = M2/sumweight
                    #    variance = variance_n * len(dataWeightPairs)/(len(dataWeightPairs) - 1)
                    
                    # calculate the weight for this column
                    my $weight = $self->getFieldWeight($vl[$i], $r);
                    
                    # skip rows with invalid or zero weights
                    next if($weight < 0.01);
                    # sum of weight values
                    $row_aux[$i] += $weight;
                    # online algorithm to calculate the mean
                    $row[$i] += ($value - $row[$i]) * $weight / $row_aux[$i];
                } elsif($type eq 'q1' or $type eq 'q2' or $type eq 'q3' or $type eq 'variance' or $type eq 'standard_error') { 
                    # descriptive statistics need postprocessing
                    unless(exists $postprocess{$i}) {
                        $postprocess{$i} = {'type' => $type, 'data' => []};
                    }
                    push @{$postprocess{$i}{'data'}[$j]}, $value;
                } elsif($type eq 'sum') { # weighted sum
                    # calculate the weight for this column
                    $row[$i] += $value * $self->getFieldWeight($vl[$i], $r);
                } elsif($type eq 'major') {
                    $row_aux[$i]{$value} += $self->getFieldWeight($vl[$i], $r);
                    $row[$i] = $value;
                    # store as the actual data value the key with the majority
                    for(keys %{$row_aux[$i]}) {
                        if($row_aux[$i]{$_} > $row_aux[$i]{$row[$i]}) {
                            $row[$i] = $_;
                        }
                    }
                } elsif($type eq 'list') { # list of values sorted by weight
                    $row_aux[$i]{$value} += $self->getFieldWeight($vl[$i], $r);
                    $row[$i] = join(' ',sort {$row_aux[$i]{$b} <=> $row_aux[$i]{$a}} keys %{$row_aux[$i]});
                } elsif($type eq 'list_weight') { # weight of each value on the list
                    $row_aux[$i]{$value} += $self->getFieldWeight($vl[$i], $r);
                    $row[$i] = join(' ', map {sprintf('%.2f', $_ / max(0.001, sum(values %{$row_aux[$i]})))} sort {$b <=> $a} values %{$row_aux[$i]});
                } elsif ($type eq 'min') { # minimum
                    if((not defined($row[$i])) or $value < $row[$i]) {
                        $row[$i] = $value;
                    }
                } elsif ($type eq 'max') { # maximum
                    if((not defined($row[$i])) or $value > $row[$i]) {
                        $row[$i] = $value;
                    }
                } else { # cell count
                    $row[$i] += 1;
                }
            }
        }
        # output the agregated row to STDOUT or to a buffer
        if(wantarray) {
            push @output, \@row;
        } else {
            # print to STDOUT using the current record and field separators
            print @row;
        }
    }
    if(%postprocess) {
        for my $i (keys %postprocess) {
            my $type = $postprocess{$i}{'type'};
            my $data = $postprocess{$i}{'data'};
            for (my $j=0;$j<=$#$data;$j++) {
                if($type eq 'q1') {
                    $output[$j][$i] = percentile($data->[$j], 0.25);
                }
                if($type eq 'q2') {
                    $output[$j][$i] = percentile($data->[$j], 0.50);
                }
                if($type eq 'q3') {
                    $output[$j][$i] = percentile($data->[$j], 0.75);
                }
                if($type eq 'variance') {
                    $output[$j][$i] = variance($data->[$j]);
                }
                if($type eq 'standard_error') {
                    $output[$j][$i] = standard_error($data->[$j]);
                }
            }
        }
    }
    return(@output);
}

sub breakdownToFile {
    my ($self, $path, @vl) = @_;
    dump_path([[breakdownHeader(@vl)],$self->breakdown(@vl)], $path);
}

# naive implementation of a percentil calculation
sub percentile {
    my ($data, $k) = @_;
    if(ref($data) ne 'ARRAY') {
        return(undef);
    }
    my $n = $#$data;
    #  || $k < 1 / (1 + $n)
    if ($n < 0) {
        return(undef);
    }
    my @s = sort {$a <=> $b} @$data;
    
    return($s[int($n * $k)]);
}

# custom mean calculation which does not consider undef values
sub mean {
    if(scalar(@_) == 0) {
        return(undef);
    }
    my $n = 0;
    my $s = 0;
    for(@_) {
        if(defined($_)) {
            $n++;
            $s += $_;
        }
    }
    return(undef) if($n == 0);
    return($s / $n);
    #return(sum(@_) / scalar(@_));
}

################################################################
# variance
#
#
# A subroutine to compute the variance of an array
# division by n-1 i s used
#
sub variance {
    my ($data) = @_;
    return(undef) if (ref($data) ne 'ARRAY' or @$data ==1);
    my $mean = mean(@$data);
    my $sqtotal = 0;
    foreach (@$data) {
        $sqtotal += ($_ - $mean) ** 2
    }
    my $var = $sqtotal / (scalar(@$data) - 1);
    return $var;
}

sub standard_deviation {
   return(sqrt(&variance));
}

sub standard_error {
    my ($data) = @_;
    return(undef) if(ref($data) ne 'ARRAY');
    my $variance = &variance;
    return(undef) unless(defined($variance));
    return(sqrt($variance) / sqrt(scalar(@$data)));
}

# calculate the weight for this row and variable
# uses a cached row and columns instead of row number and columns name to improve performance
sub getFieldWeight {
    my ($self, $vli, $row) = @_;
    my $weight = 1;
    # check if we have multipliers
    for(my $k=2;$k <= $#$vli;$k++) {
        # skip blank weights
        if (not defined($vli->[$k])) {
            # do nothing
        # numeric check, then multiply by this number
        } elsif(looks_like_number($vli->[$k])) {
            $weight *= $vli->[$k];
        } else {
            my $w = $self->get($row, $vli->[$k]);
            # ignore blank values
            if ($w == -99) {
                $weight = 0;
                last;
            }
            $weight *= $w;
        }
    }
    return($weight);
}
# convenience function to construct the column names 
# from the same vl that is used to generate the breakdown
sub breakdownHeader {
    # for each column, check if we have a alternate name
    return(map {$_->[0] =~ /\((.+)\)/ ? $1 : $_->[0]} @_);
}
# convenience function to create a list of the true column without synonyms
# from the same vl that is used to generate the breakdown
sub breakdownColumns {
    # retrive anything from the start of string up to the end or a (
    my @r = map {$_->[0] =~ /^([^\\(]+)/} @_;
    # remove duplicated names
    for(my $i=$#r;$i >= 0;$i--) {
        for(my $j=$#r;$j> $i;$j--) {
            if($r[$i] eq $r[$j]) {
                splice(@r, $j, 1);
            }
        }
    }
    return(@r);
}
# retrieve the unique row index for a given combination of breakdown values
# this function is the heart of the breakdown
# uses a branch walking algorithm that leverages Perl's very efficient hash lookup
sub getId {
    my ($self, $breakdown) = @_;
    # handle the "blank breakdown" special cases
    if((not $breakdown) || scalar(@$breakdown) == 0) {
        $breakdown = [''];
    }
    # use a hash tree to create a breakdown structure
    # start at the lowest ramification, which is a object global
    my $tree = $self->{__breakdown_tree};
    for(my $i=0;$i<=$#$breakdown;$i++) {
        # check if we need to grow a new branch
        if(not exists $tree->{$breakdown->[$i]}) {
            
            if($i == $#$breakdown) { # we are at the last possible branch, create a new leaf on this branch
                $self->{__breakdown_count} += 1;
                $tree->{$breakdown->[$i]} = $self->{__breakdown_count};
            } else { # create a new branch
                $tree->{$breakdown->[$i]} = {};
            }
        }
        # walk to the next branch of the tree
        $tree = $tree->{$breakdown->[$i]};
    }
    return($tree);
}

# simplified version of getId
# where the breakdown will only consider sequential combination of ids
sub getIdGroup {
    my ($self, $breakdown) = @_;
    # handle the "blank breakdown" special cases
    if((not $breakdown) || scalar(@$breakdown) == 0) {
        $breakdown = [''];
    }
    # simple "last row" check of keys
    # if it changed, new breakdown row
    for(my $i=0;$i<=$#$breakdown;$i++) {
        if(not exists $self->{__breakdown_tree}->{$breakdown->[$i]}) {
            # we have a new current id
            $self->{__breakdown_tree} = {map {$_ => undef} @$breakdown};
            $self->{__breakdown_count} += 1;
            last;
        }
    }
    return($self->{__breakdown_count});
}

### BREAKDOWN END ###


# display tables in a new excel workbook
# operates either in single table mode (array as input) or multi table mode (hash as input)
sub excel_tables {
    my ($input, $output) = @_;
    require Win32::OLE::Const;
    my $xlconst = Win32::OLE::Const->Load('Microsoft Excel');
    # first try to get an already opened excel then start a new instace
    my $Excel = Win32::OLE->GetActiveObject('Excel.Application') || Win32::OLE->new('Excel.Application') || return(Win32::OLE->LastError());
    # manual calculation
    my $Book = $Excel->Workbooks->Add;
    $Excel->{Calculation} = $xlconst->{xlCalculationManual};
    # multi table mode, requires a hash as input
    if(ref($input) eq 'HASH') {
        for(reverse sort keys %$input) {
            excel_add_sheet($Book, $input->{$_}, $_);
        }
    } elsif(ref($input) eq 'ARRAY') {
        excel_add_sheet($Book, $input);
    }
    # enable automatic calculation again
    $Excel->{Calculation} = $xlconst->{xlCalculationAutomatic};
    if($output) {
        $Book->SaveAs(true_path($output));
        $Book->Close(0);
    } else {
        # show the excel to the user
        $Excel->{Visible} = 1;
    }
    return($Book);
}

sub excel_add_sheet {
    my ($Book, $data, $name) = @_;
    return if(ref($data) ne 'ARRAY');
    
    my $Sheet;
    if($name) {
        # check if a sheet with the given name already exists
        for(my $i=$Book->Worksheets->Count(); $i > 0; $i--) {
            if($Book->Worksheets($i)->{Name} eq $name) {
                $Sheet = $Book->Worksheets($i);
                last;
            }
        }
    }
    # create a new blank sheet, after the last sheet
    unless($Sheet) {
        $Sheet = $Book->Worksheets->Add(undef, $Book->Worksheets($Book->Worksheets->Count()));
        if($name) {
            $Sheet->{Name} = $name;
        }
    }
    for(my $i = 0;$i<scalar(@$data);$i++) {
        for(my $j = 0;$j<scalar(@{$data->[$i]});$j++) {
            # due to some weird behavior, excel lists some text columns as 0, unless we sprintf them before
            # do a numeric type check and sprintf only the text columns
            $Sheet->Cells($i+1,$j+1)->{Value} = looks_like_number($data->[$i][$j]) ? $data->[$i][$j] : sprintf("%s",$data->[$i][$j]);
        }
    }
    return($Sheet);
}

# clean up a path and ensure its valid
# also delete any existing file !!!
sub true_path {
    my ($output) = @_;
    unless(-e $output) {
        # ensure the output file exists otherwise abs_path wont work
        # implement a touch():
        if(open(FILE,'>>', $output)) {
            close(FILE);
        }
    }
    $output = abs_path($output);
    unlink $output;
    
    # convert all / to \ to ensure the path is valid for excel
    $output =~ y/\//\\/;
    return($output);
}

### Tiebmf start ###
package Tiebmf;
use Tie::Array;
use vulcan;

sub opt_condition_sanitize {
    my ($condition, $opt) = @_;
    unless(defined($opt)) {
        $opt = {};
    }
    # convert a raw condition into a actual block select string
    if($condition =~ /^\s*\-/) {
        # check if the condition is already a select syntax
        $opt->{select} .= $condition;
    } elsif($condition =~ /\.00t$/i) {
        # bounding solid
        $opt->{select} .= ' -t "' . $condition . '"';
    } elsif($condition) {
        # user may use double quotes on text, we only allow single quotes
        $condition =~ y/"/'/;
        # user may use perl syntax, we have to convert to vulcan syntax
        $condition =~ s/\s+==\s+/ eq /g;
        $condition =~ s/\s+!=\s+/ ne /g;
        $condition =~ s/\s+<=\s+/ le /g;
        $condition =~ s/\s+>=\s+/ ge /g;
        $condition =~ s/\s+<\s+/ lt /g;
        $condition =~ s/\s+>\s+/ gt /g;

        $opt->{select} .= ' -C "' . $condition . '"';
    }
    return($opt);
}


# mandatory methods
sub TIEARRAY {
    my ($class, $path, $opt) = @_;
    my $self = {};
    bless $self;
    # default is read-only mode
    $self->{mode} = 'r';
    if(exists $opt->{mode}) {
        $self->{mode} = $opt->{mode};
    }
    # check if instead of path we received a block model object
    if(ref($path) eq 'vulcan::block_model') {
        $self->{super} = $path;
    } else {
        # create a block model object using the supplied path
        $self->{super} = new vulcan::block_model($path, $self->{mode});
    }
    
    # convert a raw condition into a actual block select string
    if(exists $opt->{condition}) {
        opt_condition_sanitize($opt->{condition}, $opt);
    }
    
    $self->{field_list} = [$self->{super}->field_list()];
    # variable for proportional evaluation on solids
    $self->{match} = 'mine';
    
    # select a subset of the model using the supplied parameters
    if(exists $opt->{select} && $opt->{select} =~ /\w+/) {
        printf STDERR ("Namedtable: select %s\n", $opt->{'select'});
        my @id_lookup;
        for($self->{super}->select("$opt->{select}");! $self->{super}->eof; $self->{super}->next) {
            # store the block and and the block match volume
            push @id_lookup, [$self->{super}->get_position(), $self->{super}->match_volume()];
        }
        $self->{id_lookup} = \@id_lookup;
        
        # check if the proportional variable exists in the model
        $self->{has_match} = $self->{super}->is_field($self->{match});
        
        # create a virtual match variable
        push @{$self->{field_list}}, $self->{match};
    } else {
        # ensure the full block model is selected
        $self->{super}->select('');
    }
    
    
    # cache the is_string property of all variables
    $self->{is_string} = {map {$_ => $self->{super}->is_string($_)} @{$self->{field_list}}};
    return($self);
}

# implements Tie FETCH method
# we also have a extra parameters to increase performance
sub FETCH {
    my ($self, $index, $vl) = @_;
    # blocks are one based, of in the case of a index 0 return the header
    if($index == 0) {
        return($self->{field_list});
    }
    # create a mask to override block values
    # the only current use is to use proportional volume when a triangulation was used to select blocks
    my %mask = ($self->{match} => 1);
    # check if we are using only a subsection of the model
    if(exists $self->{id_lookup}) {
        return([]) if($index-1 > $#{$self->{id_lookup}});
        my ($position, $match_volume) = @{$self->{id_lookup}[$index-1]};
        $self->{super}->set_position($position);
        # we have to use the prestored block volume, because it is not acurate after we use set_position
        $mask{volume} = $self->{super}->get('volume');
        # create a virtual field in case the user specified weights by the match variable 
        # but did not use any region or surfaces on the current calculation

        # prevent division by zero
        if($mask{volume} > 0) {
            if($self->{has_match}) {
                $mask{$self->{match}} = $self->{super}->get($self->{match});
            }

            $mask{$self->{match}} *= $match_volume / $mask{volume};
            $mask{volume} = $match_volume;
        }
        
    } else {
        # otherwise index translates directly to a block position
        $self->{super}->set_position($index-1);
    }
    # check if block exists
    return() if($self->{super}->eof);
    # extension to the FETCH function so we can retrieve only some columns instead of all
    unless($vl) {
        $vl = $self->{field_list};
    }
    # returns currrent block data, handling the string/numeric variables and the special volume variable for partial blocks
    return([map {if(exists $mask{$_}) {
                    $mask{$_}
                } elsif(not $_) {
                    "";
                } elsif($self->{is_string}{$_}) {
                    $self->{super}->get_string($_)
                } else {
                    $self->{super}->get($_)
                }} @$vl]);
}
# implement the $# operator of perl arrays
sub FETCHSIZE {
    my $self = shift;
    # check if we are using only a subsection of the model
    if(exists $self->{id_lookup}) {
        return($#{$self->{id_lookup}} + 2);
    } else {
        # otherwise return all blocks in the model
        return($self->{super}->n_blocks() + 1);
    }
}
# mandatory if elements writeable
sub STORE {
    my ($self, $index, $value) = @_;
    # we should implement variable adding and removal here
    if($index == 0) {
        return();
    }
    # cant store if block model was oppened as ready only
    return() if($self->{mode} eq 'r');
    # check if we are using only a subsection of the model
    if(exists $self->{id_lookup}) {
        return([]) if($index > $#{$self->{id_lookup}});
        $self->{super}->set_position($self->{id_lookup}[$index-1]);
    } else {
        # otherwise index translate directly to a block position
        $self->{super}->set_position($index-1);
    }

    # check if block exists
    return () if($self->{super}->eof);
    # check if value is a array pointer
    return() if(ref($value) ne 'ARRAY');
    for(my $i=0;$i<=$#$value;$i++) {
        if($self->{is_string}{$_}) {
            $self->{super}->put_string($self->{field_list}[$i], $value->[$i]);
        } else {
            $self->{super}->put($self->{field_list}[$i], $value->[$i]);
        }
    }
}        
#~ sub STORESIZE { ... }
#~ sub EXISTS { ... }
#~ sub DELETE { ... }

# optional methods - for efficiency
#~ sub CLEAR { ... }
#~ sub PUSH { ... }
#~ sub POP { ... }
#~ sub SHIFT { ... }
#~ sub UNSHIFT { ... }
#~ sub SPLICE { ... }
#~ sub EXTEND { ... }
#~ sub DESTROY { ... }

### Tiebmf end ###

1;

### Tiedm start ###
package Tiedm;
use Tie::Array;
use strict;

# mandatory methods
sub TIEARRAY {
    my ($class, $path, $opt) = @_;
    my $self = {};
    bless $self;
    require Win32::OLE;
    # create a empty dmfile object
    $self->{super} = Win32::OLE->new('DmFile.DmTableADO');
    unless($self->{super}) {
        print STDERR Win32::OLE->LastError(),"\n";
        return();
    }
    # use the dm object to load the input dm as readwrite
    $self->{super}->Open($path, 0);

    return() unless($self->{super}->Name);
    # create the header
    $self->{field_list} = [map {$self->{super}->Schema->GetFieldName($_)} (1 .. $self->{super}->Schema->FieldCount())];
    
    return($self);
}

sub FETCH {
    my ($self, $index, $vl) = @_;
    # blocks are one based, of in the case of a index 0 return the header
    if($index == 0) {
        return($self->{field_list});
    } elsif(exists $self->{id_lookup}) {
        $index = $self->{id_lookup}[$index];
    }
    # position the database in the requested row
    $self->{super}->MoveTo($index);
    if($vl) {
        # retrieve the specified columns
        return([map {$self->{super}->GetNamedColumn($_)} @$vl]);
    } else {
        # retrieve all columns
        return([map {$self->{super}->GetColumn($_)} (1 .. $self->{super}->Schema->FieldCount())]);
    }
}
sub FETCHSIZE {
    my $self = shift;
    if(exists $self->{id_lookup}) {
        return($#{$self->{id_lookup}} + 2);
    }
    # data plus one header row
    return($self->{super}->GetRowCount() + 1);
}
# mandatory if elements writeable
sub STORE {
    my ($self, $index, $value) = @_;
    if(exists $self->{id_lookup}) {
        $index = $self->{id_lookup}[$index];
    }
    $self->{super}->MoveTo($index);
    for(my $i=0;$i<scalar(@$value);$i++) {
        $self->{super}->SetColumn($i+1, $value->[$i]);
    }
    # save changes to disk
    $self->{super}->Update();
}        

### Tiedm end ###

### Tieisis start ###
package Tieisis;
use Tie::Array;
use vulcan;

# mandatory methods
sub TIEARRAY {
    my ($class, $path, $opt) = @_;
    my $self = {};
    bless $self;
    if(exists $opt->{mode}) {
        $self->{mode} = $opt->{mode} =~ /^w|c/ ? $opt->{mode} : 'r';
    } else {
        $self->{mode} = 'r';
    }
    # ensure database is unlocked
    unlink($path . "_lock");
    # if this lines crashes, check if any variables have underscore ("_")
    $self->{super} = new vulcan::isisdb($path, $self->{mode}, "");
    $self->{super} && $self->{super}->is_open() || die "Could not open database\n";
    $self->{path} = $path;

    if(exists $opt->{table}) {
        $self->{table} = $opt->{table};
    } else {
        # default table is the last table on the list
        ($self->{table}) = reverse($self->{super}->table_list());
    }


    #$self->{field_list} = ['KEY', map {$self->{table} . ':' . $_} $self->{super}->field_list($self->{table})];
    $self->{field_list} = ['KEY', $self->{super}->field_list($self->{table})];

    $self->{field_list_size} = scalar(@{$self->{field_list}});

    $self->{is_string} = {map {$_ => $self->{super}->is_string($_, $self->{table})} @{$self->{field_list}}};
    #print join(',',map {$_ . "=" . $self->{super}->is_string($_, $self->{table})} @{$self->{field_list}}),"\n";

    $self->{position} = [];
    for($self->{super}->rewind();! $self->{super}->eof;$self->{super}->next) {
        if($self->{super}->get_table_name() eq $self->{table}) {
            push @{$self->{position}}, $self->{super}->get_position();
        }
    }

    return($self);
}

sub FETCH {
    my ($self, $index, $vl) = @_;

    my $r = [];
    if($index == 0) {
        $r = $self->{field_list};
    } elsif($index < $self->FETCHSIZE()) {
        $self->{super}->set_position($self->{position}[$index]);
        if(ref($vl) ne 'ARRAY') {
            $vl = $self->{field_list};
        }
        for(@$vl) {
            if($_ eq 'KEY') {
                push @$r, $self->{super}->get_key();
            } elsif($self->{is_string}{$_}) {
                push @$r,  $self->{super}->get_string($_);
            } elsif($_) {
                push @$r, $self->{super}->get($_);
            }
        }
    }
    return($r);
}
sub FETCHSIZE {
    my $self = shift;
    return($#{$self->{position}} + 2);
}
# mandatory if elements writeable
sub STORE {
    my ($self, $index, $value) = @_;
    # check if database writable, index is within bounds
    if($self->{mode} ne 'r' && $index > 0 && $index < $self->FETCHSIZE()) {
        # check value is compatible with row
        if(ref($value) eq 'ARRAY' && scalar(@$value) == $self->{field_list_size}) {
            # interact with each varible on this row but the first, which is the virtual KEY field
            for(my $i = 1; $i < $self->{field_list_size}; $i++) {
                if($self->{is_string}{$self->{field_list}[$i]}) {
                    $self->{super}->put_string($self->{field_list}[$i], $value->[$i]);
                } else {
                    $self->{super}->put($self->{field_list}[$i], $value->[$i]);
                }
            }
            $self->{super}->write();
            return(0);
        }
    }
    return(1)
}

### Tieisis end ###

### TieCsv START ###
# tie a 2d array to a file to allow accessing large files
# which otherwise would not be loaded due to the memory address limit

package Tiecsv;
use Tie::File;
use Tie::Array;
use base qw(Tie::File);

# mandatory methods
sub TIEARRAY {
    my ($class, $path) = @_;
    my ($temp_handle);
    if($path) {
        $temp_handle = $path;
    } else {
        require File::Temp;
        # create a temporary file name to use a tie target
        ($temp_handle, $path) = File::Temp::tempfile();
    }
    # , recsep => "\n"
    return($class->SUPER::TIEARRAY($path, memory => 200_000_000, recsep => "\n"));
}
sub FETCH {
    my ($self, $index) = @_;
    return(unmarshall($self->SUPER::FETCH($index)));
}
#~ sub FETCHSIZE { ... }
# mandatory if elements writeable
sub STORE {
    my ($self, $index, $value) = @_;
    # print 'store',$index,marshall($value);
    # use pack to serialized perl arrays as zero separated strings, and store it on the tied file
    $self->SUPER::STORE($index, marshall($value));
}        
#~ sub STORESIZE { ... }
#~ sub EXISTS { ... }
#~ sub DELETE { ... }

# optional methods - for efficiency
#~ sub CLEAR { ... }
sub PUSH {
    my $self = shift;
    $self->SPLICE($self->FETCHSIZE, 0, @_);
}
sub POP {
    my $self = shift;
    my $size = $self->FETCHSIZE;
    return if $size == 0;
    scalar $self->SPLICE($size-1, 1);
}
sub SHIFT {
    my $self = shift;
    scalar $self->SPLICE(0, 1);
}
sub UNSHIFT {
    my $self = shift;
    $self->SPLICE(0, 0, @_);
}
sub SPLICE {
    my ($self, $index, $nrecs, @data) = @_;
    # fix a superclass bug when splicing beyond the current size
    if($index >= $self->FETCHSIZE) {
        for(my $i=0;$i<=$#data;$i++) {
            $self->STORE($index + $i, $data[$i]);
        }
    } else {
        unmarshall($self->SUPER::SPLICE($index, $nrecs, marshall(@data)));
    }
}
#~ sub EXTEND { ... }
#~ sub DESTROY { ... }

# marshall a perl array into a single line
sub marshall {
    return(map {join(',', @$_)} @_);
}

# marshall a single line of data into a perl array
sub unmarshall {
    my ($value) = @_;
    # split by comma, preserving quoted string
    # also contains a hack removing possible \r leftovers from DOS \r\n EOL, since Tie::File is set to use only \n
    [$value =~ /"[^"]*"|[^,\r]+|(?<=\A)(?=,)|(?<=,)(?=,|\Z)/ig];
}

### Tiecsv END ###



1;
