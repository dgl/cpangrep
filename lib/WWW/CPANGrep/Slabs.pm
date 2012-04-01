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
  my($self, $dist, $distname, $file, $content) = @_;

  $self->_rotate_slab if $self->_slab->full;

  $self->_slab->index($dist, $distname, $file, $content);
}

sub finish {
  my($self) = @_;

  my $r = (tied %{$self->redis})->{_conn};

  # Tie::Redis won't autovivify yet :(
  $self->redis->{$self->name} ||= [];

  push @{$self->redis->{$self->name}}, $self->_slab->name;

  for my $dist(keys %{$self->_slab->seen_dists}) {
    $r->hset("cpangrep:dists", $dist, $self->_slab->name);
  }

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
