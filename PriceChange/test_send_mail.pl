use MCCS::WMS::Sendmail;
use IBIS::Log::File;

my $go_mail = MCCS::WMS::Sendmail->new();
my $g_log = IBIS::Log::File->new( { file => '/tmp/test_send_mail', append => 1, level => 4 } );
my $g_host = `hostname`;
my $g_emails;
$g_emails->{'kav'}='kaveh.sari@usmc.mccs.org';
sub send_mail {
    my $msg_sub  = shift;
    my $msg_bod1 = shift;
    my $msg_bod2 = shift || '';
    my @body     = ( $msg_bod1, $msg_bod2 );

    return if $g_verbose;    # Dont want to send email if on verbose mode

    # $go_mail->logObj($g_log);
    $go_mail->subject($msg_sub);
    $go_mail->sendTo($g_emails);
    $go_mail->msg(@body);
    $go_mail->hostName($g_host);
    $go_mail->send_mail();
} ## end sub send_mail
send_mail('testing email','A','B');