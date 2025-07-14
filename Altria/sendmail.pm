package MCCS::SES::Sendmail;

use strict;
use warnings;
use Carp;
use List::MoreUtils qw(uniq);
use File::Basename;
use Data::Dumper;

# --- AWS SES and MIME Additions ---
use Paws;       # For AWS SDK
use MIME::Lite; # For constructing emails with attachments
use Encode qw(encode decode); # For explicit UTF-8 encoding
use MIME::Base64 qw(encode_base64); # For explicit Base64 encoding

use IBIS::Log::File;

my $pkg = __PACKAGE__;

#--------------------------------------------------------------------------
our ( @EXPORT_OK, %EXPORT_TAGS );
@EXPORT_OK = qw();
%EXPORT_TAGS = ( ALL => [@EXPORT_OK], );

#--------------------------------------------------------------------------
sub new {
    my ($class, %passed_params) = @_; 
    my $self = {};

    for my $key ( keys %passed_params ) {
        $self->{$key} = $passed_params{$key};
    }

    $self->{from_email} //= $ENV{SES_FROM_EMAIL} || 'rdistaff@usmc-mccs.org';
    $self->{aws_region} //= $ENV{AWS_REGION} || 'us-gov-west-1'; # Match your script's region
    $self->{hostName} //= $ENV{HOSTNAME} || $ENV{AWS_BATCH_JOB_ID} || "AWS";
    chomp($self->{hostName});

    if ( $passed_params{verbose} ) {
        print "$pkg new() initial state = " . Dumper $self;
    }

    eval {
        $self->{ses_client} = Paws->service('SES', region => $self->{aws_region});
    };
    if ($@) {
        my $err_msg = "Failed to initialize Paws SES client: $@. Ensure AWS credentials and region ('$self->{aws_region}') are correctly configured.";
        if ($self->{log_obj} && $self->{log_obj}->can('fatal')) {
            $self->{log_obj}->fatal($err_msg);
        }
        croak $err_msg;
    }
    
    $self->{msg_type} //= 'TEXT'; # Default message type

    bless $self, $class;
    return $self;
}

sub send_mail {
    my ($self) = @_;

    if ( $self->{verboseLevel} ) {
        my $log_msg = "Not Sending any email out on verbose = $self->{verboseLevel}";
        $self->{log_obj}->info($log_msg) if $self->{log_obj} && $self->{log_obj}->can('info');
        print qq($log_msg\n) if ( !defined $self->{log_obj} || !$self->{log_obj}->can('info') );
        return;
    }

    croak "$pkg - no email address set to Send to (sendTo)" unless defined $self->{emails} && ref $self->{emails} eq 'HASH' && scalar keys %{$self->{emails}};
    croak "$pkg - no subject set (subject)" unless defined $self->{subject};
    croak "$pkg - no message defined (msg)" unless defined $self->{msg} && ref $self->{msg} eq 'HASH' && scalar keys %{$self->{msg}};

    my $errorLvl = ( defined $self->{errorLevel} && $self->{errorLevel} ne '') ? uc( $self->{errorLevel} ) . " " : '';
    my $subject  = "$errorLvl$self->{subject}";

    my $message_body_content = "";
    foreach my $line_key ( sort keys %{ $self->{msg} } ) {
        $message_body_content .= $self->{msg}->{$line_key} . "\n";
    }
    $message_body_content .= "\nServer: $self->{hostName}\n";

    my $current_msg_type = uc($self->{msg_type} // 'TEXT');
    my $ses_body_payload;

    # Ensure body content is UTF-8 for SES
    eval { $message_body_content = encode('UTF-8', $message_body_content, Encode::FB_CROAK | Encode::LEAVE_SRC); };
    if ($@) {
        my $err_msg = "$pkg - Error UTF-8 encoding message body for SendEmail: $@";
        $self->{log_obj}->error($err_msg) if $self->{log_obj} && $self->{log_obj}->can('error');
        carp $err_msg;
        croak $err_msg;
    }

    if ($current_msg_type eq 'HTML') {
        $ses_body_payload = { Html => { Data => $message_body_content, Charset => 'UTF-8' } };
    } else {
        $ses_body_payload = { Text => { Data => $message_body_content, Charset => 'UTF-8' } };
    }

    my @recipient_addresses;
    foreach my $name ( keys %{ $self->{emails} } ) {
        push @recipient_addresses, $self->{emails}{$name};
    }
    @recipient_addresses = uniq(@recipient_addresses);

    croak "$pkg - no valid recipient email addresses found after processing." if !@recipient_addresses;
    
    if ( $self->{log_obj} && $self->{log_obj}->can('info') ) {
        $self->{log_obj}->info( "Preparing to send email via SES to: " . join(", ", @recipient_addresses) );
        $self->{log_obj}->info("   Sbj: $subject ");
    }

    # Ensure subject is UTF-8 for SES
    my $subject_encoded;
    eval { $subject_encoded = encode('UTF-8', $subject, Encode::FB_CROAK | Encode::LEAVE_SRC); };
    if ($@) {
        my $err_msg = "$pkg - Error UTF-8 encoding subject for SendEmail: $@";
        $self->{log_obj}->error($err_msg) if $self->{log_obj} && $self->{log_obj}->can('error');
        carp $err_msg;
        croak $err_msg;
    }

    my $params = {
        Destination => { ToAddresses => \@recipient_addresses },
        Message => {
            Body     => $ses_body_payload,
            Subject  => { Data => $subject_encoded, Charset => 'UTF-8' },
        },
        Source => $self->{from_email},
    };

    eval {
        my $result = $self->{ses_client}->SendEmail(%$params);
        if ( $self->{log_obj} && $self->{log_obj}->can('info') ) {
            $self->{log_obj}->info("Email sent via SES SendEmail. MessageID: " . ($result ? $result->MessageId : 'N/A'));
        }
    };
    if ($@) {
        my $error_message = "$pkg - Error sending email via SES SendEmail: $@\n";
        if (ref($@) && $@->can('message')) {
            $error_message .= "AWS SDK Error Message: " . $@->message . "\n";
            if ($@->can('code')) {
                $error_message .= "AWS SDK Error Code: " . $@->code . "\n";
            }
            if ($@->can('request_id')) {
                $error_message .= "AWS SDK Request ID: " . $@->request_id . "\n";
            }
        }
        $self->{log_obj}->error($error_message) if $self->{log_obj} && $self->{log_obj}->can('error');
        carp $error_message; 
        croak $error_message; 
    }
    
    $self->_handle_sms_queuing() if $self->{enable_sms_queuing};
    return 1;
}

sub send_mail_attachment {
    my ($self) = @_;

    $self->{msg_type} //= 'TEXT'; # Default if not set

    if ( $self->{verboseLevel} ) {
        my $log_msg = "Not Sending any email out on verbose = $self->{verboseLevel}";
        $self->{log_obj}->info($log_msg) if $self->{log_obj} && $self->{log_obj}->can('info');
        print qq($log_msg\n) if ( !defined $self->{log_obj} || !$self->{log_obj}->can('info') );
        return;
    }
    
    croak "$pkg - no email address set to Send to (sendTo)" unless defined $self->{emails} && ref $self->{emails} eq 'HASH' && scalar keys %{$self->{emails}};
    croak "$pkg - no subject set (subject)" unless defined $self->{subject};
    croak "$pkg - no message defined (msg)" unless defined $self->{msg} && ref $self->{msg} eq 'HASH' && scalar keys %{$self->{msg}};
    
    my $errorLvl = ( defined $self->{errorLevel} && $self->{errorLevel} ne '') ? uc( $self->{errorLevel} ) . " " : '';
    my $subject_line = "$errorLvl$self->{subject}";
    
    my @email_addresses_to;
    foreach my $name ( sort keys %{ $self->{emails} } ) {
        push(@email_addresses_to, $self->{emails}{$name} );
    }
    @email_addresses_to = uniq(@email_addresses_to);
    croak "$pkg - no valid recipient email addresses found after processing." if !@email_addresses_to;

    my $to_header_string = join(',', @email_addresses_to); # MIME::Lite To header

    if ( $self->{log_obj} && $self->{log_obj}->can('info') ) {
       $self->{log_obj}->info( "Preparing email with attachments via SES SendRawEmail to: " . join(", ", @email_addresses_to) );
       $self->{log_obj}->info("   Sbj: $subject_line ");
    }
    
    my $email_body_text = "";
    foreach my $line ( sort keys %{ $self->{msg} } ) {
        $email_body_text .= "$self->{msg}->{$line} \n";
    }
    $email_body_text .= "\n\nServer: $self->{hostName}";

    my $current_msg_type = uc($self->{msg_type});
    
    # Create the MIME message
    my $mime_message;
    eval {
        $mime_message = MIME::Lite->new(
            From      => $self->{from_email},
            To        => $to_header_string, # For display in email clients
            Subject   => $subject_line,     # Subject in MIME headers
            Type      => 'multipart/mixed',
        );

        # Add the text/HTML body part
        my $body_content_type = ($current_msg_type eq 'HTML') ? 'text/html' : 'text/plain';
        $mime_message->attach(
            Type     => $body_content_type,
            Data     => $email_body_text,
            Encoding => 'quoted-printable', # Good for text bodies that might have special chars
            Charset  => 'UTF-8',
        );
        
        # Add attachments
        if ( $self->{attachments} && ref $self->{attachments} eq 'ARRAY' && @{$self->{attachments}} ) {
            foreach my $full_filepath ( @{ $self->{attachments} } )  {
                unless (-f $full_filepath && -r _) { # Changed -e to -f for regular file
                    my $attach_err = "$pkg - Attachment file not found or not readable: $full_filepath. Skipping.";
                    $self->{log_obj}->warn($attach_err) if $self->{log_obj} && $self->{log_obj}->can('warn');
                    carp $attach_err;
                    next; 
                }
                my $filename_only = basename($full_filepath);
                
                # TODO: Add .dat and other types
                # Determine content type (simple guessing like in the standalone script)
                my $content_type_attach = 'application/octet-stream'; # Default
                if ($filename_only =~ /\.txt$/i) {
                    $content_type_attach = 'text/plain';
                } elsif ($filename_only =~ /\.html$/i) {
                    $content_type_attach = 'text/html';
                } elsif ($filename_only =~ /\.jpg$/i || $filename_only =~ /\.jpeg$/i) {
                    $content_type_attach = 'image/jpeg';
                } elsif ($filename_only =~ /\.png$/i) {
                    $content_type_attach = 'image/png';
                } elsif ($filename_only =~ /\.pdf$/i) {
                    $content_type_attach = 'application/pdf';
                } elsif ($filename_only =~ /\.zip$/i) {
                    $content_type_attach = 'application/zip';
                }
                # Add more types as needed

                $mime_message->attach(
                    Type        => $content_type_attach,
                    Path        => $full_filepath,
                    Filename    => $filename_only,
                    Disposition => 'attachment'
                );
                if ( $self->{log_obj} && $self->{log_obj}->can('debug') ) {
                    $self->{log_obj}->debug("Attached: $filename_only from $full_filepath (Type: $content_type_attach)");
                }
            }
        }
    };
    if ($@) {
        my $mime_err = "$pkg - Error creating MIME message: $@";
        $self->{log_obj}->error($mime_err) if $self->{log_obj} && $self->{log_obj}->can('error');
        croak $mime_err;
    }
    
    # Get the full raw email string from MIME::Lite
    my $email_string_perl = $mime_message->as_string();
    # Ensure CRLF line endings, important for MIME structure
    $email_string_perl =~ s/\r?\n/\r\n/g;

    # Explicitly encode the Perl string into a sequence of UTF-8 bytes.
    my $raw_email_data_bytes;
    eval {
        $raw_email_data_bytes = encode('UTF-8', $email_string_perl, Encode::FB_CROAK | Encode::LEAVE_SRC);
    };
    if ($@) {
        my $encode_err = "$pkg - Error UTF-8 encoding the email string for SendRawEmail: $@";
        $self->{log_obj}->error($encode_err) if $self->{log_obj} && $self->{log_obj}->can('error');
        croak $encode_err;
    }

    # Base64 encode these bytes as per SES SendRawEmail requirements.
    my $base64_encoded_data = encode_base64($raw_email_data_bytes);
    # SES expects the data for RawMessage.Data to not contain newlines from base64 formatting
    $base64_encoded_data =~ s/\n//g;


    my $send_raw_params = {
        RawMessage   => { Data => $base64_encoded_data },
        Destinations => \@email_addresses_to, # This should be an ARRAY of strings
        Source       => $self->{from_email}      # The 'From' address must be verified in SES
    };

    eval {
        my $result = $self->{ses_client}->SendRawEmail(%$send_raw_params); # Dereference hash
        if ( $self->{log_obj} && $self->{log_obj}->can('info') ) {
            $self->{log_obj}->info("Email with attachments sent via SES SendRawEmail. MessageID: " . ($result ? $result->MessageId : 'N/A'));
        }
    };
    if ($@) {
        my $error_message = "$pkg - Error sending raw email with attachments via SES: $@\n";
        if (ref($@) && $@->can('message')) {
            $error_message .= "AWS SDK Error Message: " . $@->message . "\n";
            if ($@->can('code')) {
                $error_message .= "AWS SDK Error Code: " . $@->code . "\n";
            }
            if ($@->can('request_id')) {
                $error_message .= "AWS SDK Request ID: " . $@->request_id . "\n";
            }
        }
        $self->{log_obj}->error($error_message) if $self->{log_obj} && $self->{log_obj}->can('error');
        carp $error_message;
        croak $error_message;
    }
    
    $self->_handle_sms_queuing() if $self->{enable_sms_queuing};
    return 1;
}

sub attachments {
    my ($self, @attachment_paths_or_ref) = @_;

    if (@attachment_paths_or_ref) {
        my @resolved_paths;
        if (@attachment_paths_or_ref == 1 && ref $attachment_paths_or_ref[0] eq 'ARRAY') {
            @resolved_paths = @{$attachment_paths_or_ref[0]};
        } else {
            @resolved_paths = @attachment_paths_or_ref;
        }
        
        # Filter out undef or empty strings and then get unique paths
        my @filtered = uniq(grep { defined && length } @resolved_paths);
        $self->{attachments} = \@filtered;
    } elsif (scalar @attachment_paths_or_ref == 1 && !defined $attachment_paths_or_ref[0]) {
        # Called with attachments(undef) -> clear attachments
        $self->{attachments} = [];
    } elsif (!@attachment_paths_or_ref && !exists $self->{attachments}) {
        # No argument given, and not initialized, initialize to empty array_ref
        $self->{attachments} = [];
    }
    # If called with no arguments and attachments is already set, just return it.
    # If called with attachments([]) (empty array ref), it will be handled by the first 'if' block.
    return $self->{attachments};
}

sub subject {
    my ($self, $subject_val) = @_;
    if (defined $subject_val) {
        $self->{subject} = $subject_val;
    }
    return $self->{subject};
}

sub sendTo {
    my ($self, $emails_hash) = @_;
    if (defined $emails_hash) {
        croak "$pkg - sendTo expects a HASH reference for emails, got: " . (defined $emails_hash ? ref($emails_hash) : 'undef')
            unless ref $emails_hash eq 'HASH';
        $self->{emails} = $emails_hash;
    }
    return $self->{emails};
}

sub logObj {
    my ($self, $logger_obj) = @_; 
    if (defined $logger_obj) {
        # Could add a check here to ensure $logger_obj has expected methods (info, error, etc.)
        $self->{log_obj} = $logger_obj;
    }
    return $self->{log_obj};
}

sub verboseLevel {
    my ($self, $lvl) = @_;
    if (defined $lvl) {
        $self->{verboseLevel} = $lvl;
    }
    return $self->{verboseLevel};
}

sub hostName {
    my ($self, $host_val) = @_;
    if (defined $host_val) {
        $self->{hostName} = $host_val;
        chomp($self->{hostName});
    }
    return $self->{hostName};
}

sub errorLevel {
    my ($self, $error_val) = @_;
    if (defined $error_val) {
        $self->{errorLevel} = lc($error_val); # Store as lc for consistency
    }
    return $self->{errorLevel};
}

sub msg {
    my ($self, @body_lines_or_ref) = @_;

    if (@body_lines_or_ref) {
        my $tmpHash = {};
        my @actual_lines;

        if (@body_lines_or_ref == 1 && ref $body_lines_or_ref[0] eq 'ARRAY') {
            @actual_lines = @{$body_lines_or_ref[0]};
        } elsif (@body_lines_or_ref == 1 && ref $body_lines_or_ref[0] eq 'HASH') {
            # If a hash is passed directly, use it (assuming it's already in lineXX format or similar)
            $self->{msg} = $body_lines_or_ref[0];
            return $self->{msg};
        } else {
            @actual_lines = @body_lines_or_ref;
        }
        
        my $idx = 0;
        foreach my $line (@actual_lines) {
            $tmpHash->{sprintf("line%.2d", $idx++)} = defined($line) ? $line : '';
        }
        $self->{msg} = $tmpHash;
    } elsif (!@body_lines_or_ref && !exists $self->{msg}) {
        # Initialize to empty hash if no args and not set
        $self->{msg} = {};
    }
    return $self->{msg};
}

sub message_type {
    my ($self, $type_val) = @_;
    if (defined $type_val) {
        my $uc_type = uc($type_val);
        croak "$pkg - Invalid message_type: '$type_val'. Must be 'TEXT' or 'HTML'."
            unless $uc_type eq 'TEXT' || $uc_type eq 'HTML';
        $self->{msg_type} = $uc_type;
    }
    return $self->{msg_type};
}

sub from_email {
    my ($self, $email_val) = @_;
    if (defined $email_val) {
        # Basic validation, SES will do the thorough check
        croak "$pkg - Invalid from_email format: '$email_val'" unless $email_val =~ /@/;
        $self->{from_email} = $email_val;
    }
    return $self->{from_email};
}

sub aws_region {
    my ($self, $region_val) = @_;
    if (defined $region_val && $self->{aws_region} ne $region_val) { 
        $self->{aws_region} = $region_val;
        eval {
            $self->{ses_client} = Paws->service('SES', region => $self->{aws_region});
            if ($self->{log_obj} && $self->{log_obj}->can('info')) {
                $self->{log_obj}->info("$pkg - SES client re-initialized for region '$self->{aws_region}'.");
            }
        };
        if ($@) {
            my $err_msg = "$pkg - Failed to re-initialize Paws SES client for region '$self->{aws_region}': $@.";
            $self->{log_obj}->warn($err_msg) if $self->{log_obj} && $self->{log_obj}->can('warn');
            carp $err_msg; 
        }
    }
    return $self->{aws_region};
}

sub enable_sms_queuing {
    my ($self, $enable) = @_;
    if (defined $enable) {
        $self->{enable_sms_queuing} = $enable ? 1 : 0;
    }
    return $self->{enable_sms_queuing};
}

sub _handle_sms_queuing {
    my ($self) = @_;
    
    # Ensure log_obj is available or use carp
    my $logger = $self->{log_obj};
    my $log_info = sub { $logger && $logger->can('info') ? $logger->info(@_) : CORE::print(@_, "\n"); };
    my $log_error = sub { $logger && $logger->can('error') ? $logger->error(@_) : carp(@_); };


    my ($caller_package, $caller_file) = caller(2); # Go up 2 levels to get the caller of send_mail/send_mail_attachment
    my ($program_name) = fileparse($caller_file || "unknown_caller");

    eval {
        # Check if IBIS::DBI is already loaded to avoid "Attempt to reload" warning if used elsewhere
        unless ($INC{'IBIS/DBI.pm'}) {
            require IBIS::DBI;
            IBIS::DBI->import(); 
        }

        my $db_name = $ENV{SMS_DB_NAME} || 'MVMS-Middleware-RdiUser';
        my $db_user = $ENV{SMS_DB_USER}; # May be undef if using OS auth or .pgpass
        
        my $dbh_params = { dbname => $db_name };
        $dbh_params->{user} = $db_user if $db_user; # Only add user if explicitly set

        my $dbh = IBIS::DBI->connect( %$dbh_params );
        
        unless ($dbh) {
            # IBIS::DBI->connect may not croak but return undef on failure.
            # Capture error from IBIS::DBI->errstr if available.
            my $ibis_err = IBIS::DBI->errstr || "Unknown IBIS::DBI connection error";
            croak "$pkg - Failed to connect to SMS database '$db_name': " . $ibis_err;
        }
        
        my $sql = qq(insert into sms_queue_insert (program_name, insert_date) values(?, CURRENT_TIMESTAMP) );
        my $sth = $dbh->prepare($sql);
        $sth->execute($program_name);
        $sth->finish();
        $dbh->disconnect();

        $log_info->("$pkg - SMS queue inserted for $program_name");
    };
    if ($@) {
        my $db_error = "$pkg - Error with SMS database operation: $@";
        if ($@ =~ /Can't locate IBIS\/DBI\.pm/) {
            $db_error .= " (IBIS::DBI module might not be installed or in PERL5LIB path)";
        } elsif ($@ =~ /Attempt to reload IBIS\/DBI.pm/) {
            # This is a warning, but eval catches it. Log and continue or re-throw if critical.
            $log_info->("$pkg - Note: $@"); # Log it as info, likely not fatal for SMS queuing
            return; # Or decide to croak if this shouldn't happen
        }
        $log_error->($db_error);
    }
    return;
}

#==============================================================================
# START OF DOCUMENTATION
#==============================================================================

=head1 NAME

MCCS::SES::Sendmail - A Perl module to simplify sending emails via AWS Simple Email Service (SES).

=head1 SYNOPSIS

Provides a straightforward interface for sending both simple and multipart (with attachments) emails using the AWS SES API.

 # --- Quick Start: Sending a Simple Text Email ---

 use MCCS::SES::Sendmail;

 my $mailer = MCCS::SES::Sendmail->new(
     aws_region => 'us-gov-west-1',
     from_email => 'verified-sender@example.com',
 );

 $mailer->sendTo({ 'John Doe' => 'recipient1@example.com' });
 $mailer->subject("Daily Report Status");
 $mailer->msg([
     "This is line 1 of the message.",
     "This is line 2."
 ]);

 $mailer->send_mail();


 # --- Example: Sending an Email with Attachments and High Priority ---

 use MCCS::SES::Sendmail;
 use IBIS::Log::File; # Optional logger

 my $logger = IBIS::Log::File->new(...);

 my $mailer = MCCS::SES::Sendmail->new(
     aws_region => 'us-gov-west-1',
     log_obj    => $logger, # Optional: For detailed logging
 );

 $mailer->from_email('verified-sender@example.com');
 $mailer->sendTo({
     'Jane Smith' => 'recipient2@example.com',
     'Support'    => 'support-dl@example.com',
 });
 $mailer->subject("Urgent Action Required");
 $mailer->msg("Please review the attached documents.");
 $mailer->message_type('HTML'); # Can send as HTML
 $mailer->errorLevel('HIGH');   # Prepends "HIGH" to the subject
 $mailer->attachments([
     '/path/to/report.pdf',
     '/path/to/data.csv',
 ]);

 $mailer->send_mail_attachment();


=head1 DESCRIPTION

C<MCCS::SES::Sendmail> acts as a high-level wrapper around the C<Paws> AWS SDK, specifically for the Simple Email Service (SES). It abstracts away the complexities of constructing SES API calls for both C<SendEmail> (for simple messages) and C<SendRawEmail> (for messages with attachments).

Key features include:
=over 4

=item * Simple object-oriented interface.

=item * Support for both plain text and HTML emails.

=item * Easy attachment handling using C<MIME::Lite>.

=item * Automatic handling of UTF-8 encoding for subject and body.

=item * Integration with a logging framework (like C<IBIS::Log::File>).

=item * Optional, non-critical SMS queuing feature via an internal database.

=back

=head1 METHODS

=head2 new( [%params] )

Creates and returns a new C<MCCS::SES::Sendmail> object. It initializes the AWS SES client.

 my $mailer = MCCS::SES::Sendmail->new(
     aws_region => 'us-gov-west-1',
     from_email => 'sender@example.com',
     log_obj    => $my_logger,
     verbose    => 1,
 );

The constructor accepts the following optional parameters:

=over 4

=item C<aws_region>

The AWS region where the SES service is configured. Defaults to the C<AWS_REGION> environment variable, or C<'us-gov-west-1'>.

=item C<from_email>

The email address to send from. This address B<must> be verified in AWS SES for the specified region. Defaults to the C<SES_FROM_EMAIL> environment variable, or C<'rdistaff@usmc-mccs.org'>.

=item C<log_obj>

A logger object that responds to methods like C<info>, C<error>, and C<warn>. If provided, the module will log its operations.

=item C<hostName>

The hostname to include in the email body. Defaults to C<HOSTNAME> or C<AWS_BATCH_JOB_ID> environment variables, or C<"AWS">.

=item C<verbose>

If set to a true value, prints a C<Data::Dumper> output of the object's initial state to STDOUT.

=back

The constructor will C<croak> if it fails to initialize the AWS SES client, which usually indicates an issue with AWS credentials or region configuration.

=head2 send_mail

Sends a simple email (without attachments). It uses the AWS SES C<SendEmail> API call. The method requires that C<emails>, C<subject>, and C<msg> have been set.

 $mailer->send_mail();

Returns 1 on success. Will C<croak> on validation or API errors.

=head2 send_mail_attachment

Builds and sends a multipart MIME email with one or more attachments. It uses the AWS SES C<SendRawEmail> API call. This method should be used whenever the C<attachments> attribute is set.

 $mailer->send_mail_attachment();

Returns 1 on success. Will C<croak> on validation, MIME construction, or API errors. It will warn and skip any specified attachment files that do not exist or are not readable.

=head2 subject( [ $subject_string ] )

Gets or sets the email subject line.

 my $current_subject = $mailer->subject(); # Get
 $mailer->subject("New Subject Line");   # Set

=head2 sendTo( [ $hash_ref ] )

Gets or sets the recipient email addresses. The argument B<must> be a hash reference where keys are recipient names (for clarity) and values are their email addresses.

 $mailer->sendTo({ 'User 1' => 'user1@test.com' }); # Set

=head2 msg( [ @lines | $array_ref | $hash_ref ] )

Gets or sets the body of the email. It is highly flexible:

 # Set using a list of lines
 $mailer->msg("Line 1.", "Line 2.");

 # Set using an array reference
 my @body = ("Line 1.", "Line 2.");
 $mailer->msg(\@body);

 # Set using a pre-formatted hash (advanced)
 $mailer->msg({ line00 => "Line 1.", line01 => "Line 2." });

Internally, the message is stored as a hash to preserve order. When called with no arguments, it returns the internal hash reference.

=head2 attachments( [ @paths | $array_ref ] )

Gets or sets the list of file paths for attachments. Accepts a list of paths or a single array reference of paths.

 # Set attachments
 $mailer->attachments('/path/to/file1.txt', '/path/to/file2.zip');

 # Set with an array reference
 my @files = ('/path/to/file1.txt', '/path/to/file2.zip');
 $mailer->attachments(\@files);

 # Clear attachments
 $mailer->attachments(undef);

=head2 message_type( [ 'TEXT' | 'HTML' ] )

Gets or sets the message body type. Valid values are C<'TEXT'> (default) or C<'HTML'>. Case-insensitive on set.

 $mailer->message_type('HTML');

=head2 from_email( [ $email_address ] )

Gets or sets the sender's email address. Overrides the value set in C<new()>. The address must be verified in SES.

 $mailer->from_email('another-verified-sender@example.com');

=head2 aws_region( [ $region_string ] )

Gets or sets the AWS region. If the new region is different from the current one, this method will automatically attempt to re-initialize the SES client for the new region.

 $mailer->aws_region('us-west-1');

=head2 logObj( [ $logger_object ] )

Gets or sets the logger object.

=head2 verboseLevel( [ $integer ] )

Gets or sets the verbosity level. If this is set to a true value (e.g., 1 or more), the C<send_mail> and C<send_mail_attachment> methods will log their intent and return immediately without sending an email. This is useful for debugging.

 $mailer->verboseLevel(1); # Puts the mailer in "dry run" mode

=head2 hostName( [ $hostname_string ] )

Gets or sets the hostname identifier included in the email body.

=head2 errorLevel( [ $level_string ] )

Gets or sets an error level (e.g., 'HIGH', 'MEDIUM', 'LOW'). If set, this string is prepended to the email subject line, capitalized.

 $mailer->errorLevel('High'); # Subject becomes "HIGH: Your subject"

=head2 enable_sms_queuing( [ 0 | 1 ] )

Gets or sets a boolean flag to enable or disable the SMS queuing feature. If set to true, sending an email will also trigger the C<_handle_sms_queuing> method.

 $mailer->enable_sms_queuing(1); # Enable

=head1 PRIVATE METHODS

=head2 _handle_sms_queuing

This method is called internally by C<send_mail> and C<send_mail_attachment> if C<enable_sms_queuing> is true. It attempts to connect to a database (using the internal C<IBIS::DBI> module) and insert a record into the C<sms_queue_insert> table.

This feature's configuration depends on the C<IBIS::DBI> module being in C<@INC> and the following environment variables being set:
=over 4
=item C<SMS_DB_NAME>
=item C<SMS_DB_USER>
=back

Errors in this process (e.g., module not found, DB connection failed) are logged but do not stop the email from being sent.

=head1 DEPENDENCIES

=over 4
=item * L<Paws>
=item * L<MIME::Lite>
=item * L<Encode>
=item * L<MIME::Base64>
=item * L<List::MoreUtils>
=item * L<File::Basename>
=item * L<Data::Dumper>
=item * L<Carp>

=back

=head2 Optional Dependencies

=over 4

=item * C<IBIS::Log::File> (or any compatible logger object)
=item * C<IBIS::DBI> (for the SMS queuing feature)

=back

=head1 AUTHOR

MCCS

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2025 by MCCS

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

1;