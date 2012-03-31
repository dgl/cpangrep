use Test::More;

use_ok q{WWW::CPANGrep::Search};
my $s = new_ok "WWW::CPANGrep::Search", [q => "foo"];

eval { WWW::CPANGrep::Search->new(q => "(?<=x)") };
like $@, qr/Please don't use lookbehind/;

done_testing;
