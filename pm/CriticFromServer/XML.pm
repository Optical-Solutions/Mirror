package IBIS::XML;

use strict;
use warnings;

use version; our $VERSION = qv('0.0.1');

use Carp;
use Class::Std;
use IBIS::Constants;
use XML::LibXML;

{
    my (
        %schema_of,  %document_of,   %parser_of,
        %xmlfile_of, %schemafile_of, %namespace_of,
    ) : ATTRS;

    sub BUILD
    {
        my ( $self, $oid, $arg_ref ) = @_;

        unless ( ( $arg_ref->{xmlfile} ) and ( -e $arg_ref->{xmlfile} ) )
        {
            croak 'XML file not found in ', ( caller(0) )[3];
        }

        unless (( $arg_ref->{schemafile} )
            and ( -e $arg_ref->{schemafile} ) )
        {
            croak 'XML Schema file not found in ', ( caller(0) )[3];
        }

        my $p = XML::LibXML->new();
        $p->pedantic_parser(1);
        $p->line_numbers(1);

        # XML::LibXML will die on these
        my $d = $p->parse_file( $arg_ref->{xmlfile} );
        my $s = XML::LibXML::Schema->new( location => $arg_ref->{schemafile} );

        # Nobody gets by without a valid xml document
        eval { $s->validate($d) };

        croak "XML validation error: $@ in ", ( caller(0) )[3] if $@;

        $document_of{$oid}   = $d;
        $namespace_of{$oid}  = $arg_ref->{namespace} || XMLNS;
        $parser_of{$oid}     = $p;
        $schema_of{$oid}     = $s;
        $schemafile_of{$oid} = $arg_ref->{schemafile};
        $xmlfile_of{$oid}    = $arg_ref->{xmlfile};

        return;
    }

    sub get_xmlfile
    {
        my ($self) = @_;
        return $xmlfile_of{ ident $self};
    }

    sub get_schemafile
    {
        my ($self) = @_;
        return $schemafile_of{ ident $self};
    }

    sub get_schema
    {
        my ($self) = @_;
        return $schema_of{ ident $self};
    }

    sub get_parser
    {
        my ($self) = @_;
        return $parser_of{ ident $self};
    }

    sub get_document
    {
        my ($self) = @_;
        return $document_of{ ident $self};
    }

    sub get_namespace
    {
        my ($self) = @_;
        return $namespace_of{ ident $self};
    }
}

1;

__END__

=pod

=head1 NAME

IBIS::XML - OO XML::LibXML wrapper class.

=head1 VERSION

This documentation refers to IBIS::XML version 0.0.1.

=head1 SYNOPSIS

    use XML::LibXML;

    my $xml = IBIS::XML::new->( {xmlfile => $file, schemafile => $schema} );

    my $dom = $xml->document();

=head1 DESCRIPTION

Creates a validated document object.

Requires all xml files to have a schema defined. It validates the XML against the schema before instantiating the object. You might find this cumbersome but it serves to enforce proper XML usage in IBIS.

The namespace defaults to http://www.ibis.org/schema. You may override this in the constructor. See below.

This module implements Class::Std. Note the hash ref object constructor syntax.

=head1 SUBROUTINES/METHODS

=over 4

=item new()

IBIS::XML constructor. The object created by this constructor holds references to several XML::LibXML objects. Those references are returned by calling the appropriate methods below.

There are two required parameters to the constructor: xmlfile, and schemafile. These files must exist and be able to validate. If the XML file does not validate the constructor will croak. Only valid XML is allowed under the IBIS framework.

An optional parameter is namespace. This may be used to override the default namespace defined in IBIS::Constants under XMLNS.

    my $xml = IBIS::XML->new({
        xmlfile    => $xmlfile,
        schemafile => $schemafile,
        namespace  => $namespace,
    });

=item get_parser()

Returns a reference to an XML::LibXML::Parser object.

=item get_document()

Returns a reference to an XML::LibXML::Document object.

=item get_schema()

Returns a reference to an XML::LibXML::Schema object.

=item get_xmlfile()

Returns a fully qualified XML file name.

=item get_schemafile()

Returns a fully qualified XML schema file name.

=item get_namespace()

Returns the XML namespace.

=back

=head1 INTERNAL METHODS

=over

=item BUILD

Class::Std object initializer. Creates an XML::LibXML object and validates the XML file.

=back

=head1 DIAGNOSTICS

=over 4

=item XML file not found in IBIS::XML::BEGIN

The file passed via the constructor parameter 'xmlfile' could not be found in the file system. Check your spelling and the file location.

=item XML Schema file not found in IBIS::XML::BEGIN

The file passed via the constructor parameter 'schemafile' could not be found in the file system. Check your spelling and the file location.

=item Could not parse xml file in IBIS::XML::BEGIN

There was an error from XML::LibXML when trying to create the XML::LibXML::Document object.

=item Could not create LibXML schema object in IBIS::XML::BEGIN

There was an error from XML::LibXML when trying to create the XML::LibXML::Schema object.

=item XML validation error: %s in IBIS::XML::BEGIN

The XML file does not validate against the schema. Check the XML file structure for correctness.

=back

=head1 CONFIGURATION AND ENVIRONMENT

The default namespace is configured by the constant XMLNS from IBIS::Constants.

=head1 DEPENDENCIES

    Carp    
    Class::Std
    IBIS::Constants
    XML::LibXML
    version

=head1 INCOMPATIBILITIES

None.

=head1 BUGS AND LIMITATIONS

There are no known bugs in this module.
Please report problems to Trevor S. Cornpropst L<tcornpropst@acm.org|mailto:tcornpropst@acm.org>.
Patches are welcome.

=head1 AUTHOR

Trevor S. Cornpropst B<tcornpropst@acm.org>

=head1 LICENSE AND COPYRIGHT

Copyright (c) 2006 Trevor S. Cornpropst. All rights reserved.

This module is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=head1 DISCLAIMER OF WARRANTY

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut

