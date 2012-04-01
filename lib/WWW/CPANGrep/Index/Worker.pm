package WWW::CPANGrep::Index::Worker;
use Moose;
use namespace::autoclean;

use JSON;
use Archive::Peek::Libarchive;
use File::MMagic::XS;
use WWW::CPANGrep::Slabs;
use File::Basename qw(dirname);

with 'WWW::CPANGrep::Role::RedisConnection';

has cpan_dir => (
  is => 'ro',
  isa => 'Str',
);

has slab_dir => (
  is => 'ro',
  isa => 'Str',
);

has jobs => (
  is => 'ro',
  isa => 'Int'
);

has _slab => (
  is => 'rw',
  isa => 'WWW::CPANGrep::Slabs',
  lazy => 1,
  default => sub {
    my($self) = @_;
    WWW::CPANGrep::Slabs->new(
      dir => $self->slab_dir,
      redis => $self->redis);
  }
);

has _mmagic => (
  is => 'ro',
  isa => 'File::MMagic::XS',
  default => sub {
    my $dir = dirname(__FILE__);
    # Magic shipped with File::MMagic::XS misses things like BDB databases...
    File::MMagic::XS->new($dir . "/magic");
  }
);

sub run {
  my($self, $queue) = @_;

  # Do not call ->redis before this forks.
  # (XXX: Probably should make Tie::Redis handle this somehow).

  for(1 .. ($self->jobs - 1)) {
    my $pid = fork;
    if($pid) {
      next;
    } else {
      last;
    }
  }

  my $redis_conn = (tied %{$self->redis})->{_conn};
  $redis_conn->incr("cpangrep:indexer");

  while(my $item = pop @{$self->redis->{$queue}}) {
    my($dist, $prefix) = @{decode_json $item}{qw(dist prefix)};
    $self->index_dist($dist, $prefix);
  }

  my $name = $self->_slab->finish;
  # Tie::Redis currently won't autovivify :(
  $self->redis->{"new-index"} ||= [];
  push @{$self->redis->{"new-index"}}, @{$self->redis->{$name}};

  return $redis_conn->decr("cpangrep:indexer") == 0;
}

sub index_dist{
  my($self, $dist, $prefix) = @_;
  print "Processing $dist\n";

  eval {
    Archive::Peek::Libarchive->new(
      filename => $self->cpan_dir . "/authors/id/$prefix"
    )->iterate(
      sub {
        my $file = $_[0];
        my $content = \$_[1];
        next if $file eq 'MANIFEST';

        my $mime_type = $self->_mmagic->bufmagic($$content);
        if($mime_type !~ /^text/) {
          warn "Ignoring binary file $file ($mime_type, in $dist)\n";
        } else {
          $self->_slab->index($dist, $file, $content);
        }
      }
    );
  };

  if($@) {
    warn $@;
    push @{$self->redis->{index_failures}},
      encode_json { dist => $dist, error => $@ };
  }
}

__PACKAGE__->meta->make_immutable;

1;
