#!/usr/bin/perl
use v5.10;
use Web::Simple 'CPANGrep';

package CPANGrep;
use JSON;
use POSIX qw(ceil);
use AnyEvent::Redis;
use Try::Tiny;
use HTML::Entities;
use URI::Escape qw(uri_escape_utf8);
use Data::Pageset::Render;
use HTML::Zoom;
use Config::GitLike;
require re::engine::RE2;

use constant TMPL_PATH => "share/html";
use constant MAX => 1_000;

my $config = Config::GitLike->new(confname => "cpangrep")->load_file("etc/config");

sub dispatch_request {
  \&search,
  sub (GET + /about) {
    [ 200,
    [ "Content-type" => "text/html" ], 
    [ HTML::Zoom->from_file(TMPL_PATH . "/about.html")->to_html ] ]
  },
  sub (GET + /) {
    print "Get /\n";
    open my $fh, "<", TMPL_PATH . "/grep.html" or die $!;
    [ 200,
    [ 'Content-type' => 'text/html' ],
    [ join "", <$fh> ]
    ]
  }
};

sub search(GET + / + ?q=&page~) {
  my($self, $q, $page_number) = @_;

  state $counter = 0;

  print "Search for $q\n";
  my $redis = AnyEvent::Redis->new(host => $config->{"server.queue"});

  my $cache = $redis->get("querycache:" . uri_escape_utf8($q))->recv;
  if($cache) {
  }

  my $re = eval { re_compiler($q) };

  if(!$q || !$re) {
    return [ 200, ['Content-type' => 'text/html'], [ "Sorry, I can't make sense of that. $@" ] ];
  }
  if(!$re->isa("re::engine::RE2")) {
    # XXX: friendlier errors?
    return [ 200, ['Content-type' => 'text/html'],
      [ "Please don't use lookbehind or anything else RE2 doesn't understand, grazie! <!-- " . (ref $re) . " $re -->" ] ];

  } elsif("abcdefgh" x 20 =~ /^$re$/) {
    # RE2 is quite happy with most things you throw at it, but really doesn't
    # like lots of long matches, this is just a lame check.
    return [ 200, ['Content-type' => 'text/html'],
      [ "Please don't be that greedy with your matching" ] ];
  }

  my $start = AE::time;

  my $SLAB = "stest";

  my $len = $redis->llen($SLAB)->recv;
  my $req = 6;
  my $c = int $len/$req;
  my $notify = "webfe1." . $$ . "." . ++$counter;
  for(1 .. $req) {
    my @slabs = ($c*($_-1), $_ eq $req ? $len : ($c*$_)-1);
    $req = $_, last unless @slabs;
    $redis->rpush("queue:cpangrep:slabsearch", encode_json({
        slablist => $SLAB,
        slabs => \@slabs,
        re => $q,
        notify => $notify
      }));
  }

  my $redis_other = AnyEvent::Redis->new(host => $config->{"server.slab"});
  my @results;
  # cv used to manage lifetime of subscription and zrevrangebyscore results.
  my $other_cv = AE::cv;
  $other_cv->begin;
  my $count = 0;
  $redis->subscribe($notify, sub {
      my($text) = @_;
      $count++;
      use Data::Dump qw(pp dump);
      print "Got $count results...\n" if 0 == $count % 200;
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
                #print "Match outside range!", dump($j);
                $other_cv->end;
                return;
              }
              # Clean this up.
              if($j->{snippet}->[0] < $file_offset) {
                $j->{text} = eval { substr $j->{text}, $j->{snippet}->[0] };
                $j->{snippet}->[0] -= $file_offset - $j->{snippet}->[0];
              }
              if($j->{snippet}->[1] > ($file_offset + $j->{file}->{size})) {
                $j->{text} = eval { substr $j->{text}, 0, $j->{snippet}->[1] -
                  ($file_offset + $j->{file}->{size}) };
                $j->{snippet}->[1] -= $file_offset + $j->{file}->{size};
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

  my %res = $other_cv->recv;
  my $duration = AE::time - $start;
  print "Took $duration\n";

  my $response;
  if($res{error}) {
    $response = "Something went wrong! $res{error}";
  } else {
    $response = render_response($q, \@results, $duration, $page_number)->to_html;
  }

  return [ 200, ['Content-type' => 'text/html'], [ $response ] ];
}

sub re_compiler {
  use re::engine::RE2;
  eval(q{ sub { qr/$_[0]/ } })->(shift);
}

sub render_response {
  my($q, $results, $duration, $page_number) = @_;

  my $pager = Data::Pageset::Render->new({
      total_entries    => scalar @$results,
      entries_per_page => 25,
      current_page     => $page_number || 1,
      pages_per_set    => 5,
      mode             => 'slider',
      link_format      => '<a href="?q=' . encode_entities(uri_escape_utf8($q)) . '&amp;page=%p">%a</a>'
    });

  my $output = HTML::Zoom->from_file(TMPL_PATH . "/results.html")
    ->select('title')->replace_content("$q · CPAN->grep")
    ->select('#total')->replace_content(@$results > MAX ? "more than " . MAX : scalar @$results)
    ->select('#time')->replace_content(sprintf "%0.2f", $duration)
    ->select('#start-at')->replace_content($pager->first)
    ->select('#end-at')->replace_content($pager->last)
    ->select('input[name="q"]')->add_to_attribute(value => $q);

  if(!@$results) {
    $output = $output->select('.divider')->replace_content(" ")
      ->select('.result')->replace_content("No matches found.")
      ->select('.pagination')->replace("");
  } else {
    $output = $output->select('.results')->repeat_content(
      [ map {
        my $i = $_;
        my $result = $results->[$i];
        sub {
          my($package) = $result->{file}->{dist} =~ m{([^/]+)$};
          $package =~ s/\.(?:tar\.gz|zip|tar\.bz2)$//;
          my $file = "$package/$result->{file}->{file}";
          my $author = ($result->{file}->{dist} =~ m{^([^/]+)})[0];

          # XXX: Find some better code for this.
          my $html = eval { my $html = "";
            $html .= encode_entities(substr $result->{text}, 0, $result->{match}->[0]) if $result->{match}->[0];
            $html .= "<strong>";
            $html .= encode_entities(substr $result->{text}, $result->{match}->[0], $result->{match}->[1] - $result->{match}->[0]);
            $html .= "</strong>";
            $html .= encode_entities(substr $result->{text}, $result->{match}->[1]);
            $html;
          } or do print "$@";
          $html ||= "";

          $_->select('.file-link')->replace_content($file)
          ->then
          ->set_attribute(href => "http://cpansearch.perl.org/src/$author/$file")
          ->select('.dist-link')->replace_content("$author/$package")
          ->then
          ->set_attribute(href => "http://search.cpan.org/~" . lc($author) . "/$package/")
          ->select('.excerpt')->replace_content(\$html);
        }
      } ($pager->first - 1) .. ($pager->last - 1)]);

    $output = $output->select('.pagination')->replace_content(\$pager->html);
  }

  return $output;
}

CPANGrep->run_if_script;
