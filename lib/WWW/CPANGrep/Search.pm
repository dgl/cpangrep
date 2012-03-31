package WWW::CPANGrep::Search;
use 5.014;
use AnyEvent;
use Config::GitLike;
use JSON;
use Moo;
require re::engine::RE2;
use Scalar::Util qw(blessed);
use Text::Balanced qw(gen_delimited_pat);

# TODO: stick in a module or something
my $config = Config::GitLike->new(confname => "cpangrep")->load_file("etc/config");
use constant MAX => 1_000;

has q => (
  is => 'rw',
  required => 1,
  isa => sub {
    die "Enter more characters to search for, please.\n"
      unless length $_[0] > 1;
  },
);

has _re => (
  is => 'rw',
  isa => sub { _check_re(shift) },
  coerce => sub {
    $_[0] = _re2_compile($_[0])
      unless blessed $_[0] && $_[0]->isa("re::engine::RE2");
  }
);

has _options => (
  is => 'rw',
  isa => sub { die "Expected array ref" unless ref $_[0] eq 'ARRAY' },
);

sub BUILD {
  my($self) = @_;

  my($re, $options) = _parse_search($self->q);
  $self->_re($re);
  $self->_options($options);
}

sub _parse_search {
  my($q) = @_;

  my %options = (
    file => sub {
      my($file, $type) = @_;
      { type => "file", re => $file, negate => $type eq '-' }
    },
    dist => sub {
      my($dist, $type) = @_;
      { type => "dist", re => $dist, negate => $type eq '-' }
    },
    author => sub {
      my($author, $type) = @_;
      { type => "author", re => $author, negate => $type eq '-' }
    },
  );

  my @options;
  my $opt_re = '(?:' . join('|', keys %options) . ')';
  my $arg_re = '(?:' . gen_delimited_pat(q{"/}) . '|\S+)';

  while($q =~ s/(^|\s)(?<type>-?)(?<opt>$opt_re):(?<arg>$arg_re)(?:$|\s)/$1/g) {
    my $opt = $options{$+{opt}};
    next unless $opt;
    my $type = $+{type};
    my $arg = $+{arg} =~ s/(?:^"(.*)"$|(.*))/$2||$1 =~ s{\\(.)}{$1}gr/re;
    push @options, $opt->(_re2_compile($arg), $type);
  }

  $q =~ s/\s+$//;

  return $q, \@options;
}

sub _re2_compiler {
  use re::engine::RE2 -strict => 1;
  qr/$_[0]/;
}

sub _re2_compile {
  my $re = eval { _re2_compiler($_[0]) };

  if(!$re) {
    my $error = $@;
    # RE2 says 'invalid perl operator', which is a tad confusing out of context.
    $error =~ s/perl //;
    $error =~ s/at .*\n//;
    die "Regexp '$_[0]' unparsable -- RE2 may not support the syntax. ($error)\n";
  }

  $re;
}

sub _check_re {
  my($re) = @_;

  if(!$re) {
    die "Sorry, I can't make sense of that.\n";
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
        re => "" . $self->_re,
        notify => $notify
      }));
  }

  my @finish = $other_cv->recv;
  return results => $self->filter_results(\@results), @finish;
}

sub filter_results {
  my($self, $results) = @_;
  my @results = @$results;

  for my $option(@{$self->_options}) {
    # This could probably be optimised a lot, but take the lazy approach for now.
    my $predicate = 
      $option->{type} eq 'file'   ? sub { $_->{file}->{file} =~ $option->{re} } :
      $option->{type} eq 'dist'   ? sub { $_->{file}->{dist} =~ $option->{re} } :
      $option->{type} eq 'author' ? sub { $_->{file}->{dist} =~ $option->{re} } :
      die "Unkown type";
    my $matcher = $predicate;
    $matcher = sub { !$predicate->() } if $option->{negate};
    @results = grep $matcher->(), @results;
  }

  return \@results;
}

1;
