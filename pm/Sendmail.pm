package MCCS::WMS::Sendmail;

use strict;
use Carp;
use IBIS::Log::File;
use List::MoreUtils qw(uniq);
use File::Basename;

my $pkg = __PACKAGE__;

#--------------------------------------------------------------------------
our ( @EXPORT_OK, %EXPORT_TAGS );

@EXPORT_OK = qw();

%EXPORT_TAGS = ( ALL => [@EXPORT_OK], );

#--------------------------------------------------------------------------
sub new {
    my $class  = shift;
    my %params = @_;
    my $self   = {};
    for my $key ( keys %params ) {
        $self->{$key} = $params{$key};
    }

    if ( $params{verbose} ) {
        use Data::Dumper;
        print "WMS sendmail new() = " . Dumper $self;
    }
    
    unless( defined($params{hostName})) {
	my $servername = `hostname`;
        chomp($servername);
        $self->{hostName} = $servername;
    }
     
     
     
     
    bless $self, $class;

    return $self;
}

sub send_mail {
     
     my $self = shift;

    #    use Data::Dumper;
    #    print "WMS sendmail new() = ".Dumper $self;

    if ( $self->{verboseLevel} ) {

        # Dont want to send email if on verbose modeattachments
        $self->{log_obj}->info("Not Sending any email out on verbose = $self->{verboseLevel}")
          if ( $self->{log_obj} );
        print qq(Not Sending any email out on verbose = $self->{verboseLevel})
          if ( !defined $self->{log_obj} );
        return;
    } ## end if ($g_verbose)

    if ( !defined $self->{emails} ) { die qq(Sendmail - no email address set to Send to); }
    if ( !defined $self->{subject} ) {
        die qq(Sendmail - no subject set so how are they going to know this isn't junk mail);
    }
    my $errorLvl = ( defined $self->{errorLevel} ) ? uc( $self->{errorLevel} ) : '';

    foreach my $name ( sort keys %{ $self->{emails} } ) {
        if ( $self->{log_obj} ) {
            $self->{log_obj}->info( "Sent email to $name (" . $self->{emails}{$name} . ")" );
            $self->{log_obj}->info("  Sbj:$errorLvl $self->{subject} ");
            foreach my $line ( sort keys %{ $self->{msg} } ) {
                $self->{log_obj}->info("  $line $self->{msg}->{$line}")
                  if ( $self->{log_obj} );
            }
        }

        open( MAIL, "|/usr/sbin/sendmail -t" );
        print MAIL "To: " . $self->{emails}{$name} . " \n";
        print MAIL "From: rdistaff\@usmc-mccs.org\n";
        print MAIL "Subject:$errorLvl  $self->{subject}\n";
        print MAIL "\n";
        foreach my $line ( sort keys %{ $self->{msg} } ) {
            print MAIL $self->{msg}->{$line};
            print MAIL "\n";
        }
        print MAIL "\n";
        print MAIL "Server: $self->{hostName}";
        print MAIL "\n";
        print MAIL "\n";
        close(MAIL);
    } ## end foreach my $name ( sort keys...)
    
     #---------SMS QUEUEing-------#
#     my ($package, $file) = caller(1) ;
#     my ($name,$path) = fileparse( "$file" );
#     #print qq(calling program = $name\n);
#     
#     $self->insertSMS( $name );
     #----------------------------#
} ## end sub send_mail


sub send_mail_attachment {
    use MIME::Lite;
    
    my $self = shift;
    $self->{msg_type} = 'TEXT' if (!$self->{msg_type});
    if ( $self->{verboseLevel} ) {

        # Dont want to send email if on verbose mode
        $self->{log_obj}->info("Not Sending any email out on verbose = $self->{verboseLevel}")
          if ( $self->{log_obj} );
        print qq(Not Sending any email out on verbose = $self->{verboseLevel})
          if ( !defined $self->{log_obj} );
        return;
    } ## end if ($g_verbose)
    
    if ( !defined $self->{emails} ) { die qq(Sendmail - no email address set to Send to); }
    if ( !defined $self->{subject} ) {
        die qq(Sendmail - no subject set so how are they going to know this isn't junk mail);
    }
    
    my $errorLvl = ( defined $self->{errorLevel} ) ? uc( $self->{errorLevel} ) : '';
    my @emails;
    foreach my $name ( sort keys %{ $self->{emails} } ) {
        push(@emails, qq($self->{emails}{$name}) );
    }
    
    if ( $self->{log_obj} ) {
       $self->{log_obj}->info( "Sent emails to @emails " );
       $self->{log_obj}->info("  Sbj:$errorLvl $self->{subject} ");
       foreach my $line ( sort keys %{ $self->{msg} } ) {
            $self->{log_obj}->info("  $line $self->{msg}->{$line}")
                  if ( $self->{log_obj} );
       }
    }
    
    my $msg;
    foreach my $line ( sort keys %{ $self->{msg} } ) {
        $msg .= "$self->{msg}->{$line} \n";
    }

    my $from = 'rdistaff@usmc-mccs.org';
    if ( $self->{from_email} ) {
	$from  = $self->{from_email};
    }
    my $to = join(',', @emails);
    
    my $smtp_host = 'mailhost.usmc-mccs.org';


    my $smtp = MIME::Lite->new(
        From     => $from,
        To       => $to,
        Subject  => "$errorLvl $self->{subject}",
        Type     => 'multipart/mixed', 
    );
     
#Body Section
    $smtp->attach(
        Type    => $self->{msg_type},
        Data    => "$msg \n\n\nServer: $self->{hostName}",
    );
    

#Attached File
    if ( $self->{attachments}  ) {
     foreach my $full_Filepath ( @{ $self->{attachments} } )  {
        my ($filename, $directory, $suffix) = fileparse($full_Filepath);
        
        $smtp->attach(
            Type         => 'application/text',
            Path         => $full_Filepath,
            Filename     => $filename,
            Disposition  => 'attachment', 
        );
     }
    
    }
   $smtp->send('smtp', $smtp_host, Timeout=>60);
    
     #---------SMS QUEUEing-------#
#     my ($package, $file) = caller(1) ;
#     my ($name,$path) = fileparse( "$file" );
#     #print qq(calling program = $name\n);
#     
#     $self->insertSMS( $name );
     #----------------------------#   
    
}


sub subject {
    my $self    = shift;
    my $subject = shift;

    if ($subject) {
        $self->{subject} = $subject;
    }

    return $self->{subject};

}

sub sendTo {
    my $self   = shift;
    my $emails = shift;

    if ($emails) {
        $self->{emails} = $emails;
    }

    return $self->{emails};

}

sub logObj {
    my $self    = shift;
    my $lob_obj = shift;

    if ($lob_obj) {
        $self->{log_obj} = $lob_obj;
    }

    return $self->{log_obj};

}

sub verboseLevel {
    my $self = shift;
    my $lvl  = shift;

    if ($lvl) {
        $self->{verboseLevel} = $lvl;
    }

    return $self->{verboseLevel};

}

sub hostName {
    my $self = shift;
    my $host = shift;

    if ($host) {
        $self->{hostName} = $host;
    }

    return $self->{hostName};

}

sub errorLevel {
    my $self  = shift;
    my $error = shift;

    if ($error) {
        $self->{errorLevel} = lc($error);
    }

    return $self->{errorLevel};

}

sub msg {
    my $self = shift;
    my @body = @_;

    if (@body) {
        my $tmpHash = ();
        for ( my $x = 0 ; $x < @body ; $x++ ) {
            my $suffix = sprintf( '%.2d', $x );
            ${ $self->{msg} }{"line$suffix"} = "$body[$x]";
        }
    }

    return $self->{msg};

}

sub attachments {
    my $self = shift;
    my @attachments = @_;

    if (@attachments) {
    	my @filtered = uniq(@attachments);  #Yo!! Make sure one of kind
        @{ $self->{attachments} }= @filtered;
        #TODO maybe check here to see if exists (Y/N)
    }

    return $self->{attachments};

}

sub message_type {
    my $self  = shift;
    my $type = shift;

    if ($type) {
        $self->{msg_type} = uc($type);
    } 
    return $self->{msg_type};
}



sub insertSMS {
	my $self = shift;
    my $program_name = shift;
    
	use IBIS::DBI;
	my $dbh = IBIS::DBI->connect(dbname  => 'rms_p', ) || fatal_error("Failed to connect to RMS database\n");
	
    my $sql = qq(insert into sms_queue_insert values('$program_name')  );
      
    my $sth = $dbh->prepare($sql);
       $sth->execute();
    
    
    $sth->finish();
	
}


1;
