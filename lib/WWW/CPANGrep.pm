#!/usr/bin/perl
use v5.10;
use Web::Simple 'WWW::CPANGrep';

package WWW::CPANGrep;
use AnyEvent::Redis;
use Config::GitLike;
use Data::Pageset::Render;
use HTML::Entities;
use HTML::Zoom;
use JSON;
use Scalar::Util qw(blessed);
use URI::Escape qw(uri_escape_utf8);

use WWW::CPANGrep::Search;

use constant TMPL_PATH => "share/html";
use constant MAX => 1_000;

my $config = Config::GitLike->new(confname => "cpangrep")->load_file("etc/config");

sub dispatch_request {
  sub (GET + /api) {
    sub (?q=&limit~) {
      my($self, $q, $limit) = @_;
      $limit ||= 100;
      my $r = $self->_search($q);

      _format_api($r, $limit);
    }
  },
  sub (!/api) {
    response_filter {
      $_[0] = [ 200, ['Content-type' => 'text/html'],
        [ blessed $_[0] && $_[0]->can("to_html") ? $_[0]->to_html : $_[0] ]];
    }
  },
  sub (/ + ?q=&page~) {
    my($self, $q, $page_number) = @_;

    my $r = $self->_search($q);
    # XXX: Urgh, stop abusing render_response for everything like this...
    if(ref $r eq 'HASH') {
      return render_response($q, $r->{results}, "", $r->{duration}, $page_number, $r->{count});
    } else {
      return render_response($q, undef, $r, undef);
    }
  },
  sub (/about) {
    HTML::Zoom->from_file(TMPL_PATH . "/about.html")
  },
  sub (/githook) {
    _maybe_update();
    HTML::Zoom->from_file(TMPL_PATH . "/about.html")
  },
  sub (/) {
    # XXX: Fix me if this ever becomes an overhead
    my $redis = AnyEvent::Redis->new(host => $config->{"server.queue"});
    HTML::Zoom->from_file(TMPL_PATH . "/grep.html")
      ->select('#lastupdate')
      ->replace_content($redis->get("cpangrep:lastindex")->recv);
  },
};

sub _search {
  my($self, $q) = @_;

  my $search = eval { WWW::CPANGrep::Search->new(q => $q) };
  return $@ unless $search;

  my $redis = AnyEvent::Redis->new(host => $config->{"server.queue"});
  my $start = AE::time;

  my %res;
  my $cache = $redis->get("querycache:" . uri_escape_utf8($q))->recv;
  if(!$ENV{DEBUG} && $cache) {
    %res = %{decode_json($cache)};
  } else {
    %res = $search->search($redis);
    if($res{error}) {
      return "Something went wrong! $res{error}";
    } else {
      my $redis_cache = AnyEvent::Redis->new(host => $config->{"server.queue"});
      $redis_cache->setex("querycache:" . uri_escape_utf8($q), 1800, encode_json(\%res))->recv;
    }
  }

  my $duration = AE::time - $start;
  printf "Took %0.2f %s\n", $duration, $cache ? "(cached)" : "";

  return { results => $res{results}, duration => $duration, count => $res{count} };
}

sub render_response {
  my($q, $results, $error, $duration, $page_number, $match_count) = @_;

  my $output = HTML::Zoom->from_file(TMPL_PATH . "/results.html")
    ->select('title')->replace_content("$q · CPAN->grep")
    ->select('input[name="q"]')->add_to_attribute(value => $q);

  if($error || !@$results) {
    if($error) {
      $output = $output->select('.divider')->replace_content("Error");
    } else {
      $output = $output->select('#result-count')->replace_content(" ")
        ->select('#time')->replace_content(
          $duration ? sprintf "%0.2f", $duration: "")
    }

    return $output
      ->select('.result')->replace_content($error || "No matches found.")
      ->select('.pagination')->replace("");
  }

  my $pager = Data::Pageset::Render->new({
      total_entries    => scalar @$results,
      entries_per_page => 25,
      current_page     => $page_number || 1,
      pages_per_set    => 5,
      mode             => 'slider',
      link_format      => '<a href="?q=' . encode_entities(uri_escape_utf8($q)) . '&amp;page=%p">%a</a>'
    });

  my @result_set = @$results[$pager->first - 1 .. $pager->last - 1];

  my $count = scalar @$results;
  my $count_type = "distributions";

  if($results && @$results == 1) {
    $count = $match_count;
    $count_type = "results";
  }

  $output = $output
    ->select('#time')->replace_content(sprintf "%0.2f", $duration)
    ->select('#total')->replace_content($count > MAX ? "more than " . MAX : $count)
    ->select('#count-type')->replace_content($count_type)
    ->select('#start-at')->replace_content($pager->first)
    ->select('#end-at')->replace_content(
      $count_type eq 'distributions'
        ? $pager->last
        : scalar map @{$_->{results}}, map @{$_->{files}}, @result_set);

  $output = $output->select('.results')->repeat_content(
    [ map {
      my $result = $_;
      sub {
        my($package) = $result->{dist} =~ m{([^/]+)$};
        $package =~ s/\.(?:tar\.gz|zip|tar\.bz2|tgz|tbz)$//i;
        my $author = ($result->{dist} =~ m{^([^/]+)})[0];

        $_ = $_->select('.files')->repeat_content([map {
          my $file = $_;
          sub {
            $_ = $_->select('.excerpts')->repeat_content([map {
              my $excerpt = $_;
              sub {
                $_->select('.excerpt')->replace_content(\_render_snippet($excerpt));
              }
            } @{$file->{results}}]);

            my $filename = "$package/$file->{file}";
            $_ = $_->select('.file-link')->replace_content($filename)
              ->then
              ->set_attribute(href => "https://metacpan.org/source/$author/$filename#L1");

            if($file->{truncated}) {
              $_ = $_->select('.file-number')
                ->replace_content($file->{truncated})
                ->select('.more-file')
                ->set_attribute(href => "/?q=" . uri_escape_utf8($q) . "+dist=$result->{distname}+file=$file->{file}");

              if($file->{truncated} == 1) {
                $_ = $_->select('.file-plural')->replace("");
              }
            } else {
              $_ = $_->select('.more-file')->replace("");
            }

            $_;
          }
        } @{$result->{files}}]);

        $_ = $_->select('.dist-link')->replace_content("$author/$package")
          ->then
          ->set_attribute(href => "https://metacpan.org/release/$author/$package");

        if($result->{truncated}) {
          $_ = $_->select('.dist-number')
            ->replace_content($result->{truncated})
            ->select('a.more-dist')
            ->set_attribute(href => "/?q=" . uri_escape_utf8($q) . "+dist=$result->{distname}");

          if($result->{truncated} == 1) {
            $_ = $_->select('.dist-plural')->replace("");
          }
        } else {
          $_ = $_->select('.more-dist')->replace("");
        }

        $_;
      }
    } @result_set]);

  return $output->select('.pagination')->replace_content(\$pager->html);
}

sub _format_api {
  my($r, $limit) = @_;

  my @results = grep defined, @{$r->{results}}[0 .. $limit];

  # Clean up the results a little
  # TODO: Can we just clean up the internal data structure to just match this?
  for my $result(@results) {
    for my $file(@{$result->{files}}) {
      for my $file_result(@{$file->{results}}) {
        delete $file_result->{snippet};
        delete $file_result->{zset};
        # The file thing is duplication, although size could be useful so move
        # up a level.
        my $inner_file = delete $file_result->{file};
        $file->{size} = $inner_file->{size};
      }
    }
  }

  return [200,
    ['Content-type' => 'application/json' ],
    [ to_json {
        count => $r->{count},
        duration => $r->{duration},
        results => \@results,
      }, { pretty => 1 }
    ]
  ];
}

sub _render_snippet {
  my($excerpt) = @_;
  # XXX: Find some better code for this.
  my $html = eval { my $html = "";
    $html .= encode_entities(substr $excerpt->{text}, 0, $excerpt->{match}->[0]) if $excerpt->{match}->[0];
    $html .= "<strong>";
    $html .= encode_entities(substr $excerpt->{text}, $excerpt->{match}->[0], $excerpt->{match}->[1] - $excerpt->{match}->[0]);
    $html .= "</strong>";
    $html .= encode_entities(substr $excerpt->{text}, $excerpt->{match}->[1]);
    $html;
  } or do print "$@";
  $html ||= "";
}

sub _maybe_update {
  # Slightly scary in place updating. Note there is no auth for now, someone could DoS us a
  # bit, but not much else.
  return unless -d ".git";

  my $ref = $config->{"update.ref"};
  return unless $ref;

  my $sha1 = _get_commit_id($ref);

  # Grab tags and commits, to avoid relying on git config too much
  system "git", "fetch", "origin";
  system "git", "fetch", "-t", "origin";

  # New code?
  if(_get_commit_id($ref) ne $sha1) {
    system "git", "merge", "HEAD", $ref;

    kill HUP => getppid; # starman parent process
    kill HUP => qx{pgrep -f cpangrep-matcher};
  }
}

sub _get_commit_id {
  my($ref) = @_;

  my $pid = open my $fh, "-|", qw(git log --format=%H -1), $ref or return;
  my($id) = <$fh> =~ /(\S+)/;
  waitpid $pid, 0;

  return $id;
}

WWW::CPANGrep->run_if_script;
