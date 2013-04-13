use FindBin;
use lib "$FindBin::RealBin/lib";
use WWW::CPANGrep;

WWW::CPANGrep->new->to_psgi_app;
