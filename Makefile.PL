use ExtUtils::MakeMaker;

my %opts = (
  NAME => 'cpangrep',
  PREREQ_PM => {
    'File::MMagic::XS' => 0,
    'JSON' => 0,
    'Parse::CPAN::Packages' => 0,
    'CPAN::Visitor' => 0,
    'Moose' => 0,
    'File::Slurp' => 0,
    'AnyEvent::Redis' => 0,
    'Tie::Redis' => '0.22_1',
    'Web::Simple' => 0,
    're::engine::RE2' => 0,
  }
);

WriteMakefile(%opts);
