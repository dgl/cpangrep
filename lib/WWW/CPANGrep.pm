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

      return [ 200, ['Content-type' => 'application/json' ],
               [ encode_json({
                   count => $r->{count},
                   duration => $r->{duration},
                   results => [grep defined, @{$r->{results}}[0 .. $limit]]
                 })]
             ];
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
  my($q, $results, $error, $duration, $page_number, $count) = @_;

  my $pager = Data::Pageset::Render->new({
      total_entries    => $results ? scalar @$results : 0,
      entries_per_page => 25,
      current_page     => $page_number || 1,
      pages_per_set    => 5,
      mode             => 'slider',
      link_format      => '<a href="?q=' . encode_entities(uri_escape_utf8($q)) . '&amp;page=%p">%a</a>'
    });

  my $output = HTML::Zoom->from_file(TMPL_PATH . "/results.html")
    ->select('title')->replace_content("$q · CPAN->grep")
    ->select('#total')->replace_content($count > MAX ? "more than " . MAX : $count)
    ->select('#time')->replace_content(sprintf "%0.2f", $duration || 0)
    ->select('#start-at')->replace_content($pager->first)
    ->select('#end-at')->replace_content($pager->last)
    ->select('input[name="q"]')->add_to_attribute(value => $q);

  if($error || !@$results) {
    $output = $output->select('.divider')->replace_content(" ")
      ->select('.result')->replace_content($error || "No matches found.")
      ->select('.pagination')->replace("");
  } else {
    $output = $output->select('.results')->repeat_content(
      [ map {
        my $result = $_;
        sub {
          my($package) = $result->{dist} =~ m{([^/]+)$};
          $package =~ s/\.(?:tar\.gz|zip|tar\.bz2)$//;
          my $author = ($result->{dist} =~ m{^([^/]+)})[0];

          use Data::Dump qw(pp); pp $result;

          $_ = $_->select('.files')->repeat_content([map {
            my $file = $_;
            sub {
              $_ = $_->select('.excerpts')->repeat_content([map {
                my $excerpt = $_;
                sub {
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
                  $_->select('.excerpt')->replace_content(\$html);
                }
              } @{$file->{results}}]);

              my $filename = "$package/$file->{file}";
              $_->select('.file-link')->replace_content($filename)
                ->then
                # TODO: Use metacpan here.
                ->set_attribute(href => "http://cpansearch.perl.org/src/$author/$filename");
            }
          } @{$result->{files}}]);

          $_->select('.dist-link')->replace_content("$author/$package")
            ->then
            # TODO: Use metacpan here.
            ->set_attribute(href => "http://search.cpan.org/~" . lc($author) . "/$package/");
        }
      } @$results[$pager->first - 1 .. $pager->last - 1]]);

    $output = $output->select('.pagination')->replace_content(\$pager->html);
  }

  return $output;
}

WWW::CPANGrep->run_if_script;
