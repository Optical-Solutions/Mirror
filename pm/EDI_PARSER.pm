package EDI_TWM::EDI_PARSER;
require XML::Simple;
use Data::Dumper;
use strict;
use Carp;

sub new{
    my ($class) = $_[0];
    my $obj_ref = {
    };
    bless $obj_ref, $class;
    return $obj_ref; 
}


sub create_xml_from_ref{
    my ($self, $ref, $root_name, $outfile) = @_;
    my $xs = XML::Simple->new();
    my $xml = $xs->XMLout($ref, AttrIndent => 1, RootName => $root_name);
    open(OUT, ">$outfile") || die "can not write xml file: $outfile\n";
    print OUT $xml;
    close OUT;
    return $outfile;
    ##print $xml;
}

sub parse_xml_to_ref{
    my ($self, $infile, $force_array) = @_;
    my $xs = XML::Simple->new();
    my $ref = $xs->XMLin($infile, ForceArray => $force_array);
    return $ref;
}



sub validate_array_data{
    my($self, $array_ref) = @_;
    my $validate_result;
    for(my $i=0; $i<@$array_ref; $i++){
	foreach my $key (keys %{$array_ref->[$i]}){
	    if(($array_ref->[0]->{$key}->{'nullable'} eq 'NOTNULL')&&($array_ref->[$i]->{$key}->{'d_value'} eq '')){
		$validate_result .= "$i\t"."$key\n";
	    } 
	}
    }
    return $validate_result;
}


sub validate_array_data_with_standard{
    my($self, $array_ref, $std_ref) = @_;
    my $validate_result;
    my $ctr = 0;
    for(my $i=0; $i<@$array_ref; $i++){
        foreach my $key (keys %{$array_ref->[$i]}){
            if(($std_ref->{$key}->{'nullable'} eq 'NOTNULL')&&($array_ref->[$i]->{$key} eq '')){
                $validate_result .= "$i\t"."missing\t$key\tvalue\n";
		$ctr++;
            }
        }
    }
    return ($ctr, $validate_result);
}



sub validate_level2_hash{
    my ($self, $hash_ref) = @_;
    my $validate_result;
    foreach my $key(%$hash_ref){
	if(($hash_ref->{$key}->{'nullable'} eq 'NOTNULL')&&($hash_ref->{$key}->{'d_value'} eq '')){
	    $validate_result .= "$key\n";
	}
    }
    return $validate_result;
}

sub validate_level2_hash_with_standard{
    my ($self, $hash_ref, $std_ref) = @_;
    my $validate_result;
    my $ctr = 0;
    foreach my $key(%$hash_ref){
        if(($std_ref->{$key}->{'nullable'} eq 'NOTNULL')&&($hash_ref->{$key} eq '')){
            $validate_result .= "missing\t$key\tvalue\n";
	    $ctr++;
        }
    }
    return ($ctr, $validate_result);
}

sub DESTROYER{
    my ($self) = @_;
}

1;
