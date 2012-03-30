use lib qw(lib);
use WWW::CPANGrep;

WWW::CPANGrep->new->to_psgi_app;
