package IBIS::WMS::Load::Excel::InvAdj;
use strict;
use warnings;
use Carp;
use IBIS::DBI;
use IBIS::Log::File;
use File::Basename;
use File::Path;
use Text::Iconv;
use Spreadsheet::XLSX;
use XML::Simple;
use Data::Dumper;
use IBIS::PriceChange::DB_Utils;
use Date::Calc qw(Today Add_Delta_DHMS);
use Date::Manip;
use MCCS::Config;
use File::Basename;
use Time::Local;
use Excel::Writer::XLSX;
use IBIS::E832::DB_Utils;

my $pkg                  = __PACKAGE__;
my $DEBUG                = 0;
my $g_cfg                = MCCS::Config->new();
my $g_user_support_email = $g_cfg->global_emails->{edi_user_support};

#--------------------------------------------------------------------------
sub new {
    my $type   = shift;
    my %params;
    while (my $key = shift) {
        my $value = shift;
        $params{$key} = $value;
    }
    my $self   = {};

    bless $self, $type;

    my $log_d;

    if ( $params{user} ) {
        $self->{user} = $params{user};

        #$log_d = '/usr/local/wms/log/inv_adj/' . $self->{user};
        #unless ( -d $log_d ) { mkpath($log_d); }
        $self->{db} = IBIS::PriceChange::DB_Utils->new();

    }
    else {
        croak "Need to pass username on user param";
    }

    # print map "$_ = $params{$_}\n", keys %params; # DEBUG delete this later
    if ( $params{log_object} ) {

        # use log object
        $self->{log} = $params{log_object};

    } ## end if ( $params{log_object...})
    elsif ( $params{logfile} ) {

        # use logfile;
        $self->{logfile} = $params{logfile};
        $self->{log}     = IBIS::Log::File->new(
            { file => $params{logfile}, append => 1, level => 4 } );

    } ## end elsif ( $params{logfile} )
    else {
        $self->{log} = undef;
    }

   #--------------------------------------------------------------------------
   # Do not want autocommit!
    $self->{dbh} = IBIS::DBI->connect(
        dbname => 'rms_p'
        , attribs => { AutoCommit => 0 }
    );
    $self->_prepare_sql();

    $self->{ERROR_MSG}   = undef;
    $self->{warning_msg} = '';
    $self->{db_util}     = IBIS::E832::DB_Utils->new();

    return $self;

} ## end sub new

#--------------------------------------------------------------------------
sub DESTROY {
    my $self = shift;
    ##$self->_log("[ $pkg END ]-------");

    $self->{import_sth}->finish  if defined( $self->{import_sth} );
    $self->{barcode_sth}->finish if defined( $self->{barcode_sth} );

    $self->{dbh}->disconnect;

    my $zdate = `date`;
    chomp($zdate);

    $self->_log(qq(<span class="font2">$zdate</span>));

return;
} ## end sub DESTROY

#--------------------------------------------------------------------------
sub _dev_msg {
    my @all = @_;
    foreach (@all) { s/"//g; s/\s+/ /g }
    my $str =
        qq(<span style="color: SteelBlue; font-family: Consolas, Courier New, monospace; ">Oracle error <u title="@all">message</u> for developer.</span>\n);

    return $str;
} ## end sub _dev_msg

#--------------------------------------------------------------------------
sub _prepare_sql {
    my $self = shift;

    my $sql_import = <<ENDIMPORT;
insert into wms_inv_adj_import( 
    application_user, 
    line_no, 
    transfer_id, 
    reason_id, 
    from_site, 
    to_site, 
    bar_code_id, 
    style_id, 
    color_id, 
    dimension_id, 
    size_id, 
    item_qty, 
    nfi_file
    )
    values (?,?,?,?,?,?,?,?,?,?,?,?,?)
ENDIMPORT

    $self->{import_sth} = $self->{dbh}->prepare_cached($sql_import);

    my $sql_barcode = <<ENDBAR;
select style_id, color_id, size_id, dimension_id
from bar_codes
where business_unit_id = 30
and bar_code_id = ?
ENDBAR

    $self->{barcode_sth} = $self->{dbh}->prepare_cached($sql_barcode)
        or croak("Could not prepare $sql_barcode");

return;
} ## end sub _prepare_sql

#--------------------------------------------------------------------------
sub _valid_date {
    my $self  = shift;
    my $month = shift;
    my $day   = shift;
    my $year  = shift;

    return Date::Calc::check_date( $year, $month, $day );

} ## end sub _valid_date

#--------------------------------------------------------------------------
# Put Excel data checking, all checks should be in this sub
#--------------------------------------------------------------------------
sub _checking {
    my $self    = shift;
    my $args    = shift;
    my $err_msg = '';
    my $war_msg = '';
    my $pre     = 'RSG WMS Inv Adj upload ';

    # Line record number .. where I am on the excel file.
    my $n = 3 + $args->{counter};

    #if ( $style_id ) {
    #    unless ( $self->{db_util}->is_rms_style( $style_id ) ) {
    #          $err_msg .= qq(Style "$style_id" is not RMS style.<br>\n);
    #    }
    #}

    # Barcode Checks
    #---------------
    my $b = $args->{bar_code_id};
    if ($b) {

        # sizing and format check on barcode
        if ( $b !~ m/^\d{12,13}$/ ) {
            $err_msg
                .= qq(Invalid barcode "$b". UPC is 12 digits, EAN is 13 digits.<br>\n);
        }

        if ( $self->{db_util}->is_rms_barcode($b) ) {

            # OK, it is rms barcode, then
            # let us find this barcode color, size, dim, and its style_id
            #---------------------------------------------------------
            $self->{barcode_sth}->execute($b);

            my $row = $self->{barcode_sth}->fetchrow_hashref();

            $args->{style_id}     = $row->{style_id};
            $args->{color_id}     = $row->{color_id};
            $args->{size_id}      = $row->{size_id};
            $args->{dimension_id} = $row->{dimension_id};
        }
        else {
            $err_msg .= qq(Barcode "$b" is not RMS barcode.<br>\n);
        }
    }

    # Adjustment Code aka Reason ID
    # Valid values are 1,2,3,555
    #------------------------------
    my $reason = $args->{reason_id};
    if ( $reason =~ m/^\d+$/ ) {
        if ( $reason !~ m/^1|2|3|5{3}$/ ) {
            $err_msg
                .= qq(Invalid adjustment code "$reason". The valid values are 1, 2, 3, or 555.<br>\n);
        }
    }
    else {
        $err_msg
            .= qq(Adjustment code "$reason" must be numeric only. The valid values are 1, 2, 3, or 555.<br>\n);
    }

#--------------------------------------------------------------------------------------------
# ERROR check has ended and put line no here
#--------------------------------------------------------------------------------------------
    if ($err_msg) {
        my @tmp = split( /\n/, $err_msg );
        my $z   = "";
        my $n   = 0;
        foreach (@tmp) {
            $n++;
            s/<br>//g;
            $z
                .= "<tr><td align='right' style='font-size: 8px; color: grey;'>$n.</td><td style='color: #CC0000;'>"
                . $_
                . '</td></tr>';
        }
        $self->{error_chk}->{$n}->{msg} =
            '<table cellpadding=0 cellspacing=0 border=0>' . $z . '</table>';
    } ## end if ($err_msg)

    # Warning Messages
    #-----------------
    if ( $self->{warning_msg} ) {
        $self->{warning_chk}->{$n}->{msg} = $war_msg;
    }

    if ($err_msg) {

        # Put the identification if there is error.
        return 1;
    }
    else {
        return "";
    }

}

#--------------------------------------------------------------------------
sub load_excel {

    my $self     = shift;
    my $filename = shift;
    unless ( -e $filename ) {
        croak "File \"$filename\" does not exist!";
    }

    $self->{filename} = $filename;
    my $zbase = basename($filename);
    my $zdate = `date`;
    chomp($zdate);

    my $converter = Text::Iconv->new( "utf-8", "windows-1251" );
    my $excel = Spreadsheet::XLSX->new( $filename, $converter );
    my $error_msg;
    my $db_err;
    my $distinct;

    foreach my $sheet ( @{ $excel->{Worksheet} } ) {

        #--------------------------
        # Debug .. if you have too!
        #--------------------------
        if (0) {
            $sheet->{MaxRow} ||= $sheet->{MinRow};
            printf( "Sheet: %s\n", $sheet->{Name} );
            foreach my $row ( $sheet->{MinRow} .. $sheet->{MaxRow} ) {
                $sheet->{MaxCol} ||= $sheet->{MinCol};
                foreach my $col ( $sheet->{MinCol} .. $sheet->{MaxCol} ) {
                    my $cell = $sheet->{Cells}[$row][$col];
                    if ($cell) {
                        printf( "( %s , %s ) => %s<br>\n"
                            , $row, $col, $cell->{Val} );
                    }
                } ## end foreach my $col ( $sheet->{...})
            } ## end foreach my $row ( $sheet->{...})
            next;    #  WATCH THIS !!! <--------------------
        } ## end if (0)

        #-----------------------------------------------------------
        # Parsing Here
        #-----------------------------------------------------------
        #my %values;
        my $counter      = 0;
        my $good_counter = 0;

        #----------------------
        # Row loops
        # Here the first one is on index 0, not 1.
        # ex: first col, first row is (0,0), not (1,1)
        #
        # Data starts at row 4, so index is 3
        #----------------------

        my @good_records = ();

        foreach my $row ( 3 .. $sheet->{MaxRow} ) {
            my %values = ();

            my $style_id;

            $good_counter++;

            $values{counter} = $counter;

            #if (0) {
            #    print map "$_ = $values{$_}\n", sort keys %values;
            #}
            $values{application_user} = $self->{user};

            $values{reason_id}   = $sheet->{Cells}[$row][0]->{Val};
            $values{from_site}   = $sheet->{Cells}[$row][1]->{Val};
            $values{bar_code_id} = $sheet->{Cells}[$row][2]->{Val};
            $values{item_qty}    = $sheet->{Cells}[$row][3]->{Val};

            $values{to_site} = $values{from_site};

            $values{transfer_id} = $$;
            $values{line_no}     = $values{counter};

            $values{color_id}     = '';    # Get this from barcode
            $values{size_id}      = '';    # Get this from barcode
            $values{dimension_id} = '';    # Get this from barcode
            $values{style_id}     = '';    # Get this from barcode

            $values{nfi_file} = basename($filename);

            $counter++;

            # Remove leading and trailing spaces
            # before we do any checking
            #-----------------------------------
            foreach my $k ( keys %values ) {
                if ( defined( $values{$k} ) ) {
                    $values{$k} =~ s/^\s+//g;
                    $values{$k} =~ s/\s+$//g;
                }
            } ## end foreach my $k ( keys %values)

            #$self->_log("Barcode $good_counter: " . $values{bar_code_id});

            # I am checking all records and collect all error and warning msg
            #----------------------------------------------------------------
            $error_msg .= $self->_checking( \%values );

            # Front load zeros on the style_id
            #---------------------------------
            $style_id = $values{style_id};
            if ($style_id) {
                if ( $style_id =~ m/(\d+)/ ) {    # Only want the digits
                    $style_id = $1;

                    # Front Load style_id with zeros
                    while ( length($style_id) <= 13 ) {
                        $style_id = '0' . $style_id;
                    }
                } ## end if ( $style_id =~ m/(\d+)/)
            } ## end if ($style_id)
            $values{style_id} = $style_id;

            if ($error_msg) {

                # DO NOT INSERT, we have error.
            }
            else {
                push( @good_records, \%values );

            }
        } ## end foreach my $row ( 3 .. $sheet...)

        if ($error_msg) {

            $self->_log(
                qq(<span class="font2">$zdate <span style="color: green;">$zbase</span></span> &nbsp; <span class="warning">UPLOAD FAILED</span>)
            );

            $self->_log(qq(<table class="e832_table_nh" width="100%" >));
            $self->_log("<caption>Error</caption>");
            $self->_log("<tr>");
            $self->_log(qq(    <th width="5%">Row</th>));
            $self->_log("    <th>Error Message</th>");
            $self->_log("</tr>");

            foreach my $n ( sort { $a <=> $b } keys %{ $self->{error_chk} } )
            {
                $self->_log("<tr valign=top>");
                $self->_log( '    <td align="center">' . $n . "</td>" );
                my $msg = "";
                if ( defined( $self->{error_chk}->{$n}->{msg} ) ) {
                    $msg = $self->{error_chk}->{$n}->{msg};
                    $msg =~ s/^\n+//;
                    $msg =~ s/\n+/\n/;
                    $msg =~ s/\n/<br>/g;
                } ## end if ( defined( $self->{...}))

                $self->_log(
                    '    <td style="color: #CC0000;">' . $msg . "</td>" );
                $self->_log("</tr>");

            } ## end foreach my $n ( sort { $a <=>...})

            $self->_log("</table>");

            return
                'BAD RECORD DETECTED.<p>No record uploaded.</p><p>Please see log file "'
                . basename( $self->{logfile} )
                . '"</p><br>';

            #--------------
            # BAIL OUT HERE
            #--------------

        }
        else {
            # Preinsert .. to check if we have ORACLE error
            #----------------------------------------------
            foreach my $rec (@good_records) {
                my $oracle_err = $self->_insert_one_record( %{$rec} );
                $db_err .= $oracle_err;
                $self->{dbh}->rollback;
            }
        }

        my $zbase = basename( $self->{logfile} );

        if ($db_err) {
            my $err_id = $$;
            my $str    = <<ENDOFDATA;
BAD RECORD DETECTED. No record uploaded. 
Please see log file $zbase using "View log files" link on the page.
<br>
Database Error ID = $err_id (Keep this number for reference.)
Please contact $g_user_support_email
ENDOFDATA

            $self->_log($str);
            $self->_log($db_err);
            $self->_log("+-------------+");
            $self->_log("| FAIL Upload |");
            $self->_log("+-------------+");

            return ($str);    # THERE IS RETURN HERE !

        }
        else {

       # After all the checks
       # This one is good!
       # There should be no bad records. If any, your checks need improvement.
       #----------------------------------------------------------------------
            my $msg .= "<br>\n";
            my $good_n = 0;
            my $bad_n  = 0;
            foreach my $rec (@good_records) {
                my $oracle_err = $self->_insert_one_record( %{$rec} );

                if ($oracle_err) {
                    $bad_n++;
                    $msg .= "BAD record: <br>" . $oracle_err;
                    $self->_log($oracle_err);
                    $self->{dbh}->rollback;
                }
                else {
                    $good_n++;
                    $self->{dbh}->commit;
                }
            }
            my $tot = $good_n + $bad_n;
            $msg .= "<hr noshade>";
            $msg .= "bad records: $bad_n<br>\n" if $bad_n;
            $msg .= "$good_n of $tot records uploaded.";
            $msg .= "<hr noshade>\n";

            $self->_log($msg);

            return $msg;    # RETURN HERE

        }

        last;               # Only 1 worksheet

    } ## end foreach my $sheet ( @{ $excel...})

return;
} ## end sub load_excel

#--------------------------------------------------------------------------
sub _build_warning_html {
    my $self         = shift;
    my $good_counter = shift;
    my $z_warn;
    $z_warn .= "Upload successful - ";
    $z_warn .= "$good_counter records uploaded.";
    $z_warn
        .= "Please review any included warning messages for potential concerns within the uploaded request.<br>\n";

    $z_warn .= "<h3>Warning</h3>";
    $z_warn .= qq(<table class="e832_table_nh" width="100%">);
    $z_warn .= "<tr>";
    $z_warn .= qq(    <th width="5%">Row</th>);
    $z_warn .= qq(    <th width="10%">Style Id</th>);
    $z_warn .= "    <th>Message</th>";
    $z_warn .= "</tr>";

    foreach my $n ( sort { $a <=> $b } keys %{ $self->{warning_chk} } ) {
        $z_warn .= "<tr valign=top>";
        $z_warn .= '    <td align="center">' . $n . "</td>";
        $z_warn .= "    <td>" . $self->{warning_chk}->{$n}->{style} . "</td>";

        my $msg = "";
        if ( defined( $self->{warning_chk}->{$n}->{msg} ) ) {
            $msg = $self->{warning_chk}->{$n}->{msg};
            $msg =~ s/^\n+//;
            $msg =~ s/\n+/\n/;
            $msg =~ s/\n/<br>/g;
        } ## end if ( defined( $self->{...}))

        $z_warn .= "    <td>" . $msg . "</td>";
        $z_warn .= "</tr>";

    } ## end foreach my $n ( sort { $a <=>...})

    $z_warn .= ("</table>");

    $z_warn .= "<br>\n";
    return $z_warn;
} ## end sub _build_warning_html

#--------------------------------------------------------------------------
sub _insert_one_record {
    my $self = shift;
    my %args ;    # named parameters
    while (my $key = shift) {
        my $value = shift;
        $args{$key} = $value;
    }
    eval {
        $self->{import_sth}->execute(
            $args{application_user}, $args{line_no}
            , $args{transfer_id}, $args{reason_id}
            , $args{from_site},   $args{to_site}
            , $args{bar_code_id}, $args{style_id}
            , $args{color_id},    $args{dimension_id}
            , $args{size_id},     $args{item_qty}
            , $args{nfi_file}
        );
    };

    if ($@) {
        my $a = $self->_dev_msg($@);

        return $a;
    }
    else {
        return "";
    }

} ## end sub _insert_one_record

#--------------------------------------------------------------------------
sub _insert_to_table {
    my $self = shift;
    my %args;    # name parameters
    while (my $key = shift) {
        my $value = shift;
        $args{$key} = $value;
    }
    my $end_date = $args{end_date};
    $end_date =~ s/\s+//g if defined($end_date);

    my $err_msg = $self->_insert_one_record(%args);

    return $err_msg;

}

#--------------------------------------------------------------------------
sub _log {
    my $self = shift;
    my $msg  = shift;
    if ( defined( $self->{log} ) ) {
        $self->{log}->summary($msg);
    }
    return;
} ## end sub _log

#--------------------------------------------------------------------------
1;

__END__

=pod

=head1 NAME

IBIS::WMS::Load::Excel::InvAdj- Inventory Adjustment object interface between excel and GUI upload

=head1 SYNOPSIS

    # No log
    my $object = IBIS::WMS::Load::Excel::InvAdj->new( user => $username );

    # With logfile
    my $object = IBIS::WMS::Load::Excel::InvAdj->new( user => $username, logfile=> '/log/mylogfile');


=head1 DESCRIPTION

This object interface will help you to load data into database.

=head1 CONSTRUCTOR

=over 4

=item new ( [HASH] )

IBIS::WMS::Load::Excel::InvAdj constructor is called with username, a hash assignment setting logfile which will records the operations as well as errors.  

=back

=head1 METHODS

=over 4

=item load_excel( $excel_file )

    Load the predefined template excel sheet into e832 database.
    Returns error message if any.

=back

=head1 HISTORY

=over 4

=item Thu Feb  2 08:09:53 EST 2017

Created.

=back

=head1 AUTHOR (S)

Hanny Januarius B<januariush@usmc-mccs.org>

=cut
