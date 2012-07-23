use ExtUtils::MakeMaker;

my %opts = (
  NAME => 'cpangrep',
  PREREQ_PM => {
    'File::MMagic::XS' => 0,
    'JSON' => 0,
    'Parse::CPAN::Packages' => 0,
    'Archive::Peek::Libarchive' => 0,
    'Moose' => 0,
    'MooseX::Getopt' => 0,
    'File::Slurp' => 0,
    'AnyEvent::Redis' => 0,
    'Tie::Redis' => '0.22_1',
    'Web::Simple' => 0,
    're::engine::RE2' => 0,
    'Data::Pageset::Render' => 0,
    'HTML::Zoom' => 0,
    'Config::GitLike' => 0,
    'EV' => 0,
    'IO::AIO' => 0,
    'CPAN::DistnameInfo' => 0,
    'Starman' => 0,
    'Set::Object' => 0,
    'namespace::autoclean' => 0,
  }
);

WriteMakefile(%opts);
