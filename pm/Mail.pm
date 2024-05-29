package IBIS::Mail;

use strict;
use warnings;

use version; our $VERSION = qv('0.0.1');

use Carp qw(croak);
use Net::SMTP ();

#use constant MAILHOST => 'mail.usmc-mccs.org';
use constant MAILHOST => 'mailhost.usmc-mccs.org';

sub new
{
   my($class, %args) = @_;

   return
      bless
      {
         from     => $args{from}    || undef,
         to       => $args{to}      || undef,
         cc       => $args{cc}      || undef,
         bcc      => $args{bcc}     || undef,
         sender   => $args{sender}  || undef,
         subject  => $args{subject} || undef,
         type     => $args{type}    || 'text/plain',
         body     => $args{body}    || undef,
         replyto  => $args{replyto} || undef,
      },
      $class;
}


# Defined so AUTOLOAD is not queried.
sub DESTROY {}


sub AUTOLOAD
{
   our $AUTOLOAD;
   my($self, $arg) = @_;
   my($attr) = $AUTOLOAD =~ /::([\d\w]+)$/;

   unless (defined $attr && exists $self->{$attr})
   {
      # Warrants a croak because this is a programmer error.
      croak sprintf("%s: No such attribute '$attr'", __PACKAGE__);
   }

   # Assuming interest in only the first argument.
   # Override by manual definition if otherwise.
   {  no strict 'refs';
      *{$AUTOLOAD} = sub
         {
            my($self, $arg) = @_;

            if ($#_ > 0) # undef is a valid parameter.
            {
               return $self->{$attr} = $arg;
            }
            else
            {
               return $self->{$attr};
            }
         };
   }

   return $self->$attr($arg);
}


sub send
{
   my($self, $host) = @_;
   my($smtp, @data);

   no warnings 'uninitialized';	#shut up bogus cron output 8/20/2008 ERS

   $host = defined $host ? $host : MAILHOST;

   # Be consistent with IBIS::Sendmail
   for my $attr (qw(to from subject))
   {
      croak
         sprintf("%s->send: Missing required %s field in message header",
            __PACKAGE__, ucfirst $attr)
         unless defined $self->{$attr};
   }

   # Headers from IBIS::Sendmail
   # \n is converted to \015\012 in datasend()
   @data = (
      "MIME-Version: 1.0\n",
      sprintf("Content-Type: %s; charset=\"ISO-8859-1\"\n", $self->{type}),
      "User-Agent: IBISMail/1.0\n",
      sprintf("Sender: %s\n", defined $self->{sender} ? $self->{sender} : 'IBIS System'),
      sprintf("From: %s\n", $self->{from}),
      sprintf("To: %s\n", defined $self->{to} ? join(',', @{$self->{to}}) : ''),
      sprintf("Subject: %s\n", $self->{subject})
   );
   push @data, sprintf("Reply-To: %s\n", $self->{replyto})
      if defined $self->{replyto};
   push @data, sprintf("Cc: %s\n", join(',', @{$self->{cc}}))
      if defined $self->{cc};
   push @data, sprintf("Bcc: %s\n", join(',', @{$self->{bcc}}))
      if defined $self->{bcc};

   # FIXME Verify bcc isn't sent to non-bcc'd addresses.
   # FIXME Is this the correct way to do error checking for Net::SMTP ?
   


   $smtp = Net::SMTP->new($host)
      || croak sprintf("%s->send: Net::SMTP->new: $@", __PACKAGE__);
   $smtp->mail($self->{from})
      || croak sprintf("%s->send: Net::SMTP->mail: $@", __PACKAGE__);
   $smtp->recipient(@{$self->{to}}, @{$self->{cc}}, @{$self->{bcc}})
      || croak sprintf("%s->send: Net::SMTP->recipient: $@", __PACKAGE__);

   $smtp->data || croak sprintf("%s->send: Net::SMTP->data: $@", __PACKAGE__);
   for my $line (@data, "\n",
      # FIXME Better way to break up into lines but maintain the newlines?
      defined $self->{body} ? map { "$_\n" } split(/\n/, $self->{body}) : ())
   {
      $smtp->datasend($line)
         || croak sprintf("%s->send: Net::SMTP->datasend: $@", __PACKAGE__);
   }
		  ####ERS 8/22/2008 This started throwing croak in MCCS::RMS::CostLoad... let's not check success there... we are going to Quit anyway
   $smtp->dataend; ####|| croak sprintf("%s->send: Net::SMTP->dataend: $@", __PACKAGE__);

   $smtp->quit;
     #### || croak sprintf("%s->send: Net::SMTP->quit: $@", __PACKAGE__);
}


sub print
{
   my $self = shift;
   my @data;

   # Be consistent with IBIS::Sendmail
   for my $attr (qw(to from subject))
   {
      croak
         sprintf("%s->send: Missing required %s field in message header",
            __PACKAGE__, ucfirst $attr)
         unless defined $self->{$attr};
   }

   # Headers from IBIS::Sendmail
   @data = (
      "MIME-Version: 1.0\n",
      sprintf("Content-Type: %s; charset=\"ISO-8859-1\"\n", $self->{type}),
      "User-Agent: IBISMail/1.0\n",
      sprintf("Sender: %s\n", defined $self->{sender} ? $self->{sender} : 'IBIS System'),
      sprintf("From: %s\n", $self->{from}),
      sprintf("To: %s\n", defined $self->{to} ? join(',', @{$self->{to}}) : ''),
      sprintf("Subject: %s\n", $self->{subject})
   );
   push @data, sprintf("Reply-To: %s\n", $self->{replyto})
      if defined $self->{replyto};
   push @data, sprintf("Cc: %s\n", join(',', @{$self->{cc}}))
      if defined $self->{cc};
   push @data, sprintf("Bcc: %s\n", join(',', @{$self->{bcc}}))
      if defined $self->{bcc};

   print @data, "\n", defined $self->{body} ? $self->{body} : '';
}


1;

__END__

=pod

=head1 NAME

IBIS::Mail - OO module for creating and sending email

=head1 SYNOPSIS

   use IBIS::Mail ();
   my $mail = IBIS::Mail->new(
      to=>['j00@j0urb0x'], from=>'m3h@m3hb0x',
      subject=>'gr33tz', body=>'t3h m3ss4g3'
   );
   $mail->print if $debug;
   $mail->send; # Croaks on error.

=head1 DESCRIPTION

IBIS::Mail is an object oriented interface for creating and sending email.

=head1 CONSTRUCTOR

=over 4

=item new(fields ..)

All object fields are available in the constructor.  These include:

=over 4

=item from

From address, a scalar.

=item to

To addresses, an array reference.

=item cc

Cc addresses, an array reference.

=item bcc

Bcc addresses, an array reference.

=item subject

Subject, a scalar.

=item body

Body of message, a scalar.

Long messages are easily done using HERE documents:

   my $body =<<END
   y0 wh4t t3s up?
   ch3ck m3h l4t3st spl01t:
   ad infinitum ..
   END

=back

=back

=head1 METHODS

All of the object fields described above are available as accessor/mutator
methods.  Pass no parameters for an accessor.  For mutators the first parameter
replaces the existing value.

=over 4

=item send([MAILHOST])

Sends the email if the object has a defined from, to, and subject fields.
Optionally can specify an alternate mailhost to use.  Croaks on error.

=item print

Prints the mail as it will be sent.

=back

=head1 BUGS

None yet, beyond the lame leetspeak.

=head1 AUTHORS

Joey Makar B<makarjo@usmc-mccs.org>

=cut
