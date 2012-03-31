package WWW::CPANGrep::Search;
use 5.014;
use AnyEvent;
use Config::GitLike;
use JSON;
use Moo;
use Scalar::Util qw(blessed);

# TODO: stick in a module or something
my $config = Config::GitLike->new(confname => "cpangrep")->load_file("etc/config");
use constant MAX => 1_000;

has q => (
  is => 'ro',
  required => 1,
  isa => sub {
    die "Enter more characters to search for, please.\n"
      unless length $_[0] > 1;
  }
);

has re => (
  is => 'ro',
  isa => sub { _check_re(shift) },
  default => sub { shift->q },
  coerce => sub {
    use re::engine::RE2;
    $_[0] = eval(q{ sub { qr/$_[0]/ } })->($_[0])
      unless blessed $_[0] && $_[0]->isa("re::engine::RE2");
  }
);

sub _check_re {
  my($re) = @_;

  if(!$re) {
    die "Sorry, I can't make sense of that.\n";
  } elsif(!$re->isa("re::engine::RE2")) {
    die "Please don't use lookbehind or anything else RE2 doesn't understand.\n";
  }

  my($min, $max) = $re->possible_match_range;
  if(($min eq $max and length $min < 3) || "abcdefgh" x 20 =~ $re) {
    # RE2 is quite happy with most things you throw at it, but really doesn't
    # like lots of long matches, this is just a lame check.
    die "Please don't be that greedy with your matching.\n";
  }
}

sub search {
  my($self, $redis) = @_;
  state $counter = 0;

  my @results;
  my $slab = $config->{"key.slabs"};
  my $len = $redis->llen($slab)->recv;
  my $req = $config->{"matcher.concurrency"};
  $req = $len if $len < $req;
  my $notify = "webfe1." . $$ . "." . ++$counter;

  my $redis_other = AnyEvent::Redis->new(host => $config->{"server.slab"});
  # cv used to manage lifetime of subscription and zrevrangebyscore results.
  my $other_cv = AE::cv;
  $other_cv->begin;
  my $count = 0;
  $redis->subscribe($notify, sub {
      my($text) = @_;
      $count++;
      return if not $text;
      my $j = decode_json($text);

      if($count > MAX) {
        # quite enough, thanks
        $redis->unsubscribe($notify);
        $other_cv->end; # don't want to wait for unsubscribe to happen, hence this other CV..
      }

      if(ref $j eq 'HASH' && $j->{done}) {
        $req-- if $j->{done};
        if(!$req) { 
          $redis->unsubscribe($notify);
          $other_cv->end; # don't want to wait for unsubscribe to happen, hence this other CV..
        }
      } elsif(ref $j eq 'HASH' && $j->{error}) {
        $other_cv->send(error => $j->{error});
      } else {
        eval {
          my $j = $_;
          # Note the ending offset of the match is used here, we want to see
          # where the end of the match was, just incase it went over two files
          # which shouldn't actually be shown to the user.
          $redis_other->zrevrangebyscore($j->{zset}, $j->{match}->[1], "-inf",
            "withscores", "limit", 0, 1, sub {
              my($file_info) = @_;
              my($file, $file_offset) = @$file_info;
              $j->{file} = decode_json $file;
              if($j->{match}->[0] < $file_offset || $j->{match}->[1] > $file_offset + $j->{file}->{size}) {
                $other_cv->end;
                return;
              }
              # XXX: Clean this up.
              if($j->{snippet}->[0] < $file_offset) {
                print "($j->{snippet}->[0] < $file_offset)\n";
                $j->{text} = substr $j->{text}, $file_offset - $j->{snippet}->[0];
                $j->{snippet}->[0] += $file_offset - $j->{snippet}->[0];
                $j->{snippet}->[1] -= $file_offset - $j->{snippet}->[0];
              }
              if($j->{snippet}->[1] > ($file_offset + $j->{file}->{size})) {
                # XXX: logic messed up
                my $removed = length($j->{text}) - ($file_offset + $j->{file}->{size});
                $j->{text} = substr $j->{text}, 0, $j->{snippet}->[1] -
                  ($file_offset + $j->{file}->{size});
                $j->{snippet}->[1] -= $removed;
              }

              # Finally normalise the match so it's an offset within the
              # snippet
              $j->{match}->[0] -= $j->{snippet}->[0];
              $j->{match}->[1] -= $j->{snippet}->[0];

              push @results, $j;
              $other_cv->end;
            });
          $other_cv->begin;
        } for @$j;
        if($@) {
          warn $@;
        }
      }
    });

  my $c = int $len/$req;
  for(1 .. $req) {
    my @slabs = ($c*($_-1), $_ eq $req ? $len : ($c*$_)-1);
    $redis_other->rpush("queue:cpangrep:slabsearch", encode_json({
        slablist => $slab,
        slabs => \@slabs,
        re => "" . $self->re,
        notify => $notify
      }));
  }

  return results => \@results, $other_cv->recv;
}

1;
