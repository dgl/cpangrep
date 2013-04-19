package WWW::CPANGrep::Slab::Writer;
use Moose;
use namespace::autoclean;

use File::Slurp;
use JSON;

use WWW::CPANGrep::Slab::Common;

my $COUNTER = 0;

has dir => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

has redis => (
  is => 'ro',
  isa => 'Tie::Redis::Connection',
  required => 1,
);

has name => (
  is => 'ro',
  isa => 'Str',
  default => sub { "slab:zset:$$-" . ++$COUNTER },
);

has rotate_size => (
  is => 'ro',
  isa => 'Int',
  default => sub { 10 * 1024 * 1024 }, # 10mb
);

has seen_dists => (
  is => 'ro',
  isa => 'HashRef',
  default => sub { {} },
);

has size => (
  is => 'rw',
  isa => 'Int',
  default => 0,
);

has _fh => (
  is => 'ro',
  isa => 'GlobRef',
  lazy => 1,
  default => sub {
    my($self) = @_;
    open my $fh, ">", $self->dir . "/" . $self->name or die $!;
    binmode $fh;
    $fh;
  },
);

sub BUILDARGS {
  my($self, %args) = @_;

  # For speed avoid using the tied interface
  $args{redis} = (tied %{$args{redis}})->{_conn};

  return \%args;
}

sub index {
  my($self, $dist, $distname, $file, $content) = @_;

  if($$content =~ /^.*\0/) { # first line contains NUL => probably binary
    warn "Ignoring probable binary file $file (in $dist)";
    return;
  }

  print {$self->_fh} $$content, SLAB_SEPERATOR;

  $self->redis->zadd($self->name, $self->size, encode_json {
      size     => length($$content),
      dist     => $dist,
      distname => $distname,
      file     => $file
  });

  $self->{seen_dists}{$distname}++;

  $self->size($self->size + length($$content) + length SLAB_SEPERATOR);
}

sub full {
  my($self) = @_;
  return $self->size >= $self->rotate_size;
}

__PACKAGE__->meta->make_immutable;

1;
