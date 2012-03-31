package WWW::CPANGrep::Index;
use Config::GitLike;
use Moose;
use namespace::autoclean;
use Parse::CPAN::Packages;
use WWW::CPANGrep::Index::Worker;
use FindBin ();
use Cwd 'abs_path';

with 'MooseX::Getopt';
with 'WWW::CPANGrep::Role::RedisConnection';

my $config = Config::GitLike->new(
  confname => "cpangrep"
)->load_file("$FindBin::RealBin/../etc/config");

has cpan_dir => (
  is => 'ro',
  isa => 'Str',
  default => sub { abs_path $config->{"location.cpan"} },
  documentation => "Directory where CPAN mirror resides",
);

has slab_dir => (
  is => 'ro',
  isa => 'Str',
  default => sub { abs_path $config->{"location.slabs"} },
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

  if($self->redis->{"cpangrep:indexer"}) {
    warn "Semaphore not 0, previous run failed / in progress?";
  }

  $self->redis->{"cpangrep:indexer"} = 0;

  delete $self->redis->{"new-index"};

  my $done = WWW::CPANGrep::Index::Worker->new(
    cpan_dir => $self->cpan_dir,
    slab_dir => $self->slab_dir,
    redis_server => $self->redis_server,
    jobs => $self->jobs,
  )->run($queue);

  if($done) {
    my $redis_conn = (tied %{$self->redis})->{_conn};
    eval { $redis_conn->rename("cpangrep:slabs", "cpangrep:slabs-old") };
    $redis_conn->rename("new-index", "cpangrep:slabs");
    $redis_conn->save;

    for my $slab(@{$self->redis->{"cpangrep:slabs-old"}}) {
      unlink $self->slab_dir . "/" . $slab;
    }

    $self->redis->{"cpangrep:lastindex"} = $packages->last_updated;
  }
}

__PACKAGE__->meta->make_immutable;

1;
