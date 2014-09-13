package WWW::CPANGrep::Matcher;
use Config::GitLike;
use FindBin ();
use JSON;
use Moose;
use WWW::CPANGrep::Slab::Common;
use 5.010;
use POSIX ();
use Time::HiRes qw(time);
use EV;
use AnyEvent::Redis;
use IO::AIO qw(mmap aio_readahead);
use re::engine::RE2;
use namespace::autoclean;

with 'MooseX::Getopt';

my $config = Config::GitLike->new(
  confname => "cpangrep"
)->load_file("$FindBin::RealBin/../etc/config");

has slab_dir => (
  is => 'ro',
  isa => 'Str',
  default => sub {
    File::Spec->rel2abs($config->{"location.slabs"}, "$FindBin::RealBin/..");
  },
  documentation => "Directory in which to save 'slabs' extracted from CPAN",
);

has jobs => (
  is => 'ro',
  isa => 'Int',
  default => $config->{"matcher.concurrency"},
  documentation => "Number of jobs to run (default 10)",
);

has _redis => (
  is => 'ro',
  isa => 'Object',
  reader => 'redis',
  default => sub { AnyEvent::Redis->new; },
);

sub match {
  my($self) = @_;

  # XXX: Use some module for this, this is silly
  if($self->jobs > 1) {
    for(1 .. $self->jobs) {
      my $pid = fork;
      if($pid) {
        if($_ == $self->jobs) { exit }
        next;
      } else {
        last;
      }
    }
  }

  print "$$: ready\n";

  my $HUP_set = POSIX::SigSet->new(POSIX::SIGHUP);
  $SIG{HUP} = sub {
    # (Arguably due to a bug in perl's signal handling) we end up with the
    # signal still blocked after the exec(), so unblock it manually now.
    POSIX::sigprocmask(POSIX::SIG_UNBLOCK, $HUP_set);
    exec $^X, $0, 1;
    die "exec() failed: $!";
  };

  while(1) {
    while(my $item = $self->redis->blpop("queue:cpangrep:slabsearch", 60)->recv) {
      last unless $item->[0];
      POSIX::sigprocmask(POSIX::SIG_BLOCK, $HUP_set);
      print "$$: processing job: $item->[1]\n";
      my $job = decode_json $item->[1];
      my $slabs = [map $self->redis->lindex($job->{slablist}, $_)->recv, @{$job->{slabs}}];
      my $max = $job->{max} || 500;
      my $start = time;
      $self->do_match($job->{re}, $max, $job->{notify}, $slabs);
      $self->redis->publish($job->{notify} => encode_json {
        done => 1,
        id => $job->{id}});
      my $end = time;
      print "$$: job done (", $end-$start, "s)\n";
      POSIX::sigprocmask(POSIX::SIG_UNBLOCK, $HUP_set);
    }
    $self->redis->ping->recv;
  }
}

sub open_cached {
  my($file) = @_;
  state %cache;

  open $cache{$file}, "<", $file or do {
    %cache = ();
    open $cache{$file}, "<", $file or die "$file: $!";
  } unless $cache{$file};
  return $cache{$file};
}

sub do_match {
  my($self, $re, $max, $channel, $process) = @_;

  my $matches = 0;

  $re = qr/$re/m;

  my $i = 0;
  my $fh_next = open_cached($self->slab_dir . "/" . $process->[$i++]);

  for my $file(@$process) {
    my $fh = $fh_next;
    mmap my $pm, -s $fh, IO::AIO::PROT_READ, IO::AIO::MAP_SHARED, $fh or die $!;

    if(my $next = $process->[$i++]) {
      $fh_next = open_cached($self->slab_dir . "/" . $next);
      # On machines with spare IO bandwidth this seemed to help, however I'm now
      # running on VMs and this seems less of a help.
      #aio_readahead $fh_next, 0, -s $next;
    }

    my @results;
    while($pm =~ /$re/gm) {
      if($+[0] - $-[0] > 1e5) {
        $self->redis->publish($channel => encode_json {
          error => "Regexp is too greedy"
        });
         return;
      }

      # XXX: A bit broken in edge cases.
      # Be careful not to use regexps!
      my $previous = rindex($pm, "\n", $-[0]);
      $previous = 1+rindex($pm, "\n", $previous-1) if $previous > 0;
      my $next = index($pm, "\n", $+[0]);
      $next = index($pm, "\n", 1+$next) if $next > 0;

      # Limit length of snippet, 200 bytes should be enough for anyone
      if($next > $previous + 200) {
        $previous = $previous < $-[0] - 100 ? $-[0] - 100 : $previous;
        $next = $next > $+[0] + 100 ? $+[0] + 100 : $next;
      }

      my $match = [$-[0], $+[0]];

      # Calculate matching line numbers which, unlike the returned snippet and
      # match range, are adjusted for the _indexed_ file contained within the
      # slab file.  Adjustment happens here rather than in WWW::CPANGrep::search()
      # because it lacks easy and fast access to the slab or indexed file
      # content for line counting.
      my $indexed_file_offset = rindex($pm, SLAB_SEPERATOR, $match->[0]);
      if ($indexed_file_offset >= 0) {
          $indexed_file_offset += length SLAB_SEPERATOR;
      } else {
          $indexed_file_offset = 0;
      }

      my $text  = substr($pm, $previous, $next - $previous);
      my $start = 1 + substr($pm, $indexed_file_offset, $match->[0] - $indexed_file_offset) =~ tr/\n//;
      my $end   = $start + (substr($pm, $match->[0], $match->[1] - $match->[0]) =~ tr/\n//);

      push @results, {
        zset    => $file,
        text    => $text,
        snippet => [$previous, $next],
        match   => $match,
        line    => [$start, $end],
      };

      last if ++$matches > $max;
    }

    $self->redis->publish($channel => encode_json \@results);

    return if $matches > $max;
  }
}

1;
