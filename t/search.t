use Test::More;

use_ok q{WWW::CPANGrep::Search};
my $s = new_ok "WWW::CPANGrep::Search", [q => "foo file:test.pm dist:dist.foo -dist=bar author:dgl"];

{
  use re::engine::RE2;

  is ref $s->{_re}, 're::engine::RE2';
  is $s->{_re}, qr/foo/;
  is_deeply $s->{_options}, [
    { type => "file", negate => "", re => qr/test.pm/ },
    { type => "dist", negate => "", re => qr/dist.foo/ },
    { type => "dist", negate => 1, re => qr/^bar$/ },
    { type => "author", negate => "", re => qr/DGL/ },
  ];
}                            

eval { WWW::CPANGrep::Search->new(q => "(?<=x)") };
like $@, qr/RE2 may not/;

done_testing;
