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
    sub (?q=&limit~&exclude_file~) {
      my($self, $q, $limit, $exclude_file) = @_;
      $limit ||= 100;
      my $r = $self->_search($q, $exclude_file);

      return [ 200, ['Content-type' => 'application/json' ],
               [ encode_json({
                   count => scalar @{$r->{results}},
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
  sub (/ + ?q=&page~&exclude_file~) {
    my($self, $q, $page_number, $exclude_file) = @_;

    my $r = $self->_search($q, $exclude_file);
    # XXX: Urgh, stop abusing render_response for everything like this...
    if(ref $r eq 'HASH') {
      return render_response($q, $r->{results}, "", $r->{duration}, $page_number);
    } else {
      return render_response($q, undef, $r, undef);
    }
  },
  sub (/about) {
    HTML::Zoom->from_file(TMPL_PATH . "/about.html")
  },
  sub (/) {
    HTML::Zoom->from_file(TMPL_PATH . "/grep.html")
  },
};

sub _search {
  my($self, $q, $exclude_file) = @_;

  my $search = eval { WWW::CPANGrep::Search->new(q => $q) };
  return $@ unless $search;

  my $redis = AnyEvent::Redis->new(host => $config->{"server.queue"});
  my $start = AE::time;

  my $results;
  my $response;
  my $cache = $redis->get("querycache:" . uri_escape_utf8($q))->recv;
  if(!$ENV{DEBUG} && $cache) {
    $results = decode_json($cache);
  } else {
    my %res = $search->search($redis);
    if($res{error}) {
      $response = "Something went wrong! $res{error}";
      return $response;
    } else {
      my $redis_cache = AnyEvent::Redis->new(host => $config->{"server.queue"});
      $redis_cache->setex("querycache:" . uri_escape_utf8($q), 1800, encode_json($res{results}))->recv;
      $results = $res{results};
    }
  }

  #if($exclude_file) {
  #  $exclude_file = re_compiler($exclude_file);
  #  $results = [grep $_->{file}->{file} !~ $exclude_file, @{$results}];
  #}

  my $duration = AE::time - $start;
  printf "Took %0.2f %s\n", $duration, $cache ? "(cached)" : "";

  return { results => $results, duration => $duration };
}

sub render_response {
  my($q, $results, $error, $duration, $page_number) = @_;

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
    ->select('#total')->replace_content(!$results || @$results > MAX ? "more than " . MAX : scalar @$results)
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
          # TODO: Use metacpan here.
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

WWW::CPANGrep->run_if_script;
