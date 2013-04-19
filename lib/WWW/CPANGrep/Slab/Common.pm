package WWW::CPANGrep::Slab::Common;

use base "Exporter";
our @EXPORT = qw( SLAB_SEPERATOR );

use constant SLAB_SEPERATOR => "\n\0\1\2\x{e}\x{0}\x{f}\2\1\0\n";

1;
