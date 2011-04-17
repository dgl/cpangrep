package WWW::CPANGrep::Role::RedisConnection;
use Moose::Role;
use Tie::Redis;

has redis_server => (
  is => 'ro',
  isa => 'Str',
  default => 'localhost',
  documentation => "Where to connect to Redis (host[:port], default localhost:6379)",
);

has _redis => (
  is => 'ro',
  isa => 'HashRef',
  reader => 'redis',
  default => sub {
    my($self) = @_;
    my($host, $port) = ($self->redis_server =~ /^(.+)(?::(\d+))?$/);
    $port ||= 6379;

    tie my %h, "Tie::Redis", host => $host, port => $port;
    return \%h;
  },
  lazy => 1
);

1;
