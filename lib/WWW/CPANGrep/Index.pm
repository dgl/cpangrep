package WWW::CPANGrep::Index;
use Moose;
use namespace::autoclean;
use Parse::CPAN::Packages;
use WWW::CPANGrep::Index::Worker;

with 'MooseX::Getopt';
with 'WWW::CPANGrep::Role::RedisConnection';

has cpan_dir => (
  is => 'ro',
  isa => 'Str',
  required => 1,
  documentation => "Directory where CPAN mirror resides",
);

has slab_dir => (
  is => 'ro',
  isa => 'Str',
  required => 1,
  documentation => "Directory in which to save 'slabs' extracted from CPAN",
);

has jobs => (
  is => 'ro',
  isa => 'Int',
  default => 10,
  documentation => "Number of jobs to run (default 10)",
);

sub index {
  my($self) = @_;

  my $packages = Parse::CPAN::Packages->new($self->cpan_dir
    . "/modules/02packages.details.txt.gz");

  my $queue = "distlist:queue:" . time;
  my @queue = map $_->cpanid . "/" . $_->filename,
        $packages->latest_distributions;

  $self->redis->{$queue} = \@queue;
  print "Inserted ", scalar(@{$self->redis->{$queue}}), " dists into $queue\n";

  delete $self->redis->{"new-index"};

  WWW::CPANGrep::Index::Worker->new(
    cpan_dir => $self->cpan_dir,
    slab_dir => $self->slab_dir,
    redis_server => $self->redis_server,
  )->run($queue);
}

__PACKAGE__->meta->make_immutable;

1;
