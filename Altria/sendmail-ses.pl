#!/usr/bin/perl

#==============================================================================
#
# FILE: sendmail-ses.pl
#
# USAGE: /home/perl/bin/SES-TEST/sendmail-ses.pl
# .tpl: _deploy/ia/mvms-dev-sendmail-ses-batch-job-definition.tpl
#
# DESCRIPTION:
#   A comprehensive test script and usage example for the MCCS::SES::Sendmail
#   module. It demonstrates how to send simple emails, emails with
#   attachments, and HTML emails.
#
# REQUIREMENTS:
#   - MCCS::SES::Sendmail module and its dependencies installed.
#   - AWS credentials configured in the environment (e.g., via ~/.aws/credentials
#     or IAM role).
#       - Add to your .tpl (see "_deploy\ia\mvms-dev-sendmail-ses-batch-job-definition.tpl" for an example):		
#            "jobRoleArn": "${jobRoleArn}",   
#   - The sender email address must be verified in the target AWS SES region.
#
# IMPORTANT:
#   You MUST configure the variables in the "Configuration for Test"
#   section below before running.
#
#==============================================================================

use strict;
use warnings;
use MCCS::SES::Sendmail; # Make sure this module is in your @INC or PERL5LIB path
use Data::Dumper;
use File::Spec;
use Encode qw(decode);

# --- Configuration for Test ---
# IMPORTANT: Replace with your actual VERIFIED emails and desired region for testing.
# The 'from_email' MUST be verified in AWS SES in the specified 'aws_region'.

# The email address that will receive the test emails.
my $test_to_email   = $ENV{'TEST_EMAIL_RECIPIENT'} || 'rdistaff@usmc-mccs.org';
# The email address that will send the emails. MUST BE A VERIFIED SENDER IN AWS SES.
my $test_from_email = $ENV{'TEST_EMAIL_SENDER'}    || 'rdistaff@usmc-mccs.org';
# The AWS region where your SES service is configured and the sender is verified.
my $test_aws_region = $ENV{'TEST_AWS_REGION'}      || 'us-gov-west-1';

# --- Setup: Create dummy attachment files for the test ---
# This section creates temporary files to be used as attachments.
my $temp_dir = File::Spec->tmpdir();
my $attachment1_path = File::Spec->catfile($temp_dir, "test_report_module.txt");
my $attachment2_path = File::Spec->catfile($temp_dir, "image_module.png");
my $attachment3_path = File::Spec->catfile($temp_dir, "archive_module.zip");

# Create a text file with unicode characters.
open my $fh_txt, '>', $attachment1_path or die "Cannot create $attachment1_path: $!";
print $fh_txt "This is a test report generated at " . localtime() . ".\n";
print $fh_txt "It includes some special characters: € £ ¥ α β γ.\n";
close $fh_txt;

# Create placeholder files for other common types.
open my $fh_png, '>', $attachment2_path or die "Cannot create $attachment2_path: $!";
print $fh_png "This is not a real PNG, just a test file.\n";
close $fh_png;

open my $fh_zip, '>', $attachment3_path or die "Cannot create $attachment3_path: $!";
print $fh_zip "This is not a real ZIP, just a test file.\n";
close $fh_zip;

print "Dummy attachment files created/verified:\n";
print "- $attachment1_path\n";
print "- $attachment2_path\n";
print "- $attachment3_path\n\n";


#==============================================================================
# --- Test 1: Email with Attachments ---
# Demonstrates sending a multi-part email using send_mail_attachment().
# It also shows how the module handles a non-existent attachment path gracefully.
#==============================================================================
print "--- Test 1: Sending email with attachments ---\n";
eval {
    # Initialize the mailer. 'verbose => 1' is useful for seeing the initial state.
    my $mailer = MCCS::SES::Sendmail->new(
        aws_region => $test_aws_region,
        # log_obj => YourOptionalLogger->new(), # You could pass a logger object here
        verbose    => 1 # Print Dumper of initial state
    );

    # Set email parameters using accessor methods.
    $mailer->from_email($test_from_email);
    $mailer->sendTo({ 'Test Recipient' => $test_to_email });
    $mailer->subject("Module Test: Email with Attachments & Unicode €ħαρß - " . time());
    $mailer->msg([
        "Hello from the MCCS::SES::Sendmail module!",
        "This email includes attachments and Unicode characters like: € £ ¥ α β γ.",
        "Another line with some more text.",
        "And a final line for the body."
    ]);
    # Provide a list of attachments. The module will warn about the non-existent file and skip it.
    $mailer->attachments([$attachment1_path, $attachment2_path, $attachment3_path, "/tmp/nonexistent_file.doc"]);
    $mailer->message_type('TEXT');
    # $mailer->errorLevel('HIGH'); # Optional: Prepends "HIGH " to subject

    # Set verboseLevel to 0 to actually send. A value of 1 or more would skip sending.
    $mailer->verboseLevel(0);

    print "Mailer object configured:\n" . Dumper($mailer) . "\n";

    # Use send_mail_attachment() because we have attachments.
    if ($mailer->send_mail_attachment()) {
        print "Test 1: Email with attachments allegedly sent successfully!\n";
    } else {
        print "Test 1: Failed to send email with attachments (as per return code).\n";
    }

};
if ($@) {
    # Catch any fatal errors (croak) from the module.
    my $error = $@;
    eval { $error = decode('UTF-8', $error, Encode::FB_WARN) }; # Ensure error prints correctly
    print "Test 1: DIED while trying to send email with attachments: $error\n";
}


#==============================================================================
# --- Test 2: Simple email (no attachments) ---
# Demonstrates sending a basic email using send_mail().
# Also shows setting parameters directly in the constructor.
#==============================================================================
print "\n--- Test 2: Simple email (no attachments) ---\n";
eval {
    # This time, set 'from_email' directly in new()
    my $mailer_simple = MCCS::SES::Sendmail->new(
        aws_region => $test_aws_region,
        from_email => $test_from_email
    );

    $mailer_simple->sendTo({ 'Simple Test Recipient' => $test_to_email });
    $mailer_simple->subject("Module Test: Simple Email Unicode €ħαρß - " . time());
    # This demonstrates passing a hash to msg() instead of an array ref.
    $mailer_simple->msg({
        line00 => "This is a simple test email from the module.",
        line01 => "No attachments here. Just plain text (or HTML if type is set).",
        line02 => "Unicode test: € £ ¥."
    });
    $mailer_simple->message_type('TEXT');
    $mailer_simple->verboseLevel(0); # 0 means send

    print "Mailer object for simple email configured:\n" . Dumper($mailer_simple) . "\n";

    # Use send_mail() because there are no attachments.
    if ($mailer_simple->send_mail()) {
        print "Test 2: Simple email allegedly sent successfully!\n";
    } else {
        print "Test 2: Failed to send simple email (as per return code).\n";
    }
};
if ($@) {
    my $error = $@;
    eval { $error = decode('UTF-8', $error, Encode::FB_WARN) };
    print "Test 2: DIED while trying to send simple email: $error\n";
}


#==============================================================================
# --- Test 3: HTML Email with Attachments ---
# Demonstrates sending a message with an HTML body by setting message_type('HTML').
#==============================================================================
print "\n--- Test 3: Sending HTML email with attachments ---\n";
eval {
    my $mailer_html = MCCS::SES::Sendmail->new(
        aws_region => $test_aws_region,
        from_email => $test_from_email
    );

    $mailer_html->sendTo({ 'HTML Test Recipient' => $test_to_email });
    $mailer_html->subject("Module Test: HTML Email with Attachments & Unicode €ħαρß - " . time());
    
    # Create an HTML body.
    my $html_body = <<'EOF_HTML';
<h1>Hello from MCCS::SES::Sendmail!</h1>
<p>This is an <b>HTML</b> email with attachments.</p>
<p>It includes Unicode characters like: &euro; &pound; &yen; &alpha; &beta; &gamma;.</p>
<p>And even some direct UTF-8: こんにちは世界</p>
<ul>
    <li>Item 1</li>
    <li>Item 2</li>
</ul>
EOF_HTML

    # Set the message body and type.
    $mailer_html->msg([$html_body]);
    $mailer_html->attachments([$attachment1_path]);
    $mailer_html->message_type('HTML'); # Set the type to HTML
    $mailer_html->verboseLevel(0); 

    print "Mailer object for HTML email configured:\n" . Dumper($mailer_html) . "\n";

    # Use send_mail_attachment() for HTML + attachment.
    if ($mailer_html->send_mail_attachment()) {
        print "Test 3: HTML Email with attachments allegedly sent successfully!\n";
    } else {
        print "Test 3: Failed to send HTML email with attachments (as per return code).\n";
    }

};
if ($@) {
    my $error = $@;
    eval { $error = decode('UTF-8', $error, Encode::FB_WARN) }; # Try to decode if it's a byte string from a die
    print "Test 3: DIED while trying to send HTML email with attachments: $error\n";
}


print "\n--- All tests completed. Check your inbox ($test_to_email) and logs. ---\n";
print "Remember to delete dummy files if no longer needed:\n";
print "- $attachment1_path\n";
print "- $attachment2_path\n";
print "- $attachment3_path\n";

exit 0;