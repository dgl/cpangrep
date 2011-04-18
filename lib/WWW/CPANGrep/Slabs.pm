package WWW::CPANGrep::Slabs;
use Moose;
use namespace::autoclean;

use JSON; # XXX: Implement serialisation in Tie::Redis to avoid this

use WWW::CPANGrep::Slab::Writer;

has dir => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

has redis => (
  is => 'ro',
  isa => 'HashRef',
  required => 1,
);

has _slab => (
  is => 'rw',
  isa => 'Maybe[WWW::CPANGrep::Slab::Writer]',
  lazy => 1,
  default => sub {
    my($self) = @_;
    WWW::CPANGrep::Slab::Writer->new(
      redis => $self->redis, 
      dir => $self->dir
    );
  },
);

has name => (
  is => 'ro',
  isa => 'Str',
  default => sub { "slabs:set:process:$$" }
);

sub index {
  my($self, $dist, $file) = @_;

  $self->_rotate_slab if $self->_slab->full;

  $self->_slab->index($dist, $file);
}

sub finish {
  my($self) = @_;

  # Tie::Redis won't autovivify yet :(
  $self->redis->{$self->name} ||= [];

  push @{$self->redis->{$self->name}}, encode_json {
    file => $self->_slab->file_name,
    zset => $self->_slab->zset_name
  };

  $self->_slab(undef);

  return $self->name;
}

sub _rotate_slab {
  my($self) = @_;

  $self->finish;

  $self->_slab(WWW::CPANGrep::Slab::Writer->new(
      dir => $self->dir,
      redis => $self->redis
  ));
}

__PACKAGE__->meta->make_immutable;

1;
