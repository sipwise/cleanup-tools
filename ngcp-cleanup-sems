#!/usr/bin/perl

use strict;
use warnings;
use Redis;

my $max_days = 2;
my $monit_restart = 1;

lprint("=====================");
lprint("script start");

# find bind IP from redis config file

my ($server_ip, $port);

{
	my $fd;
	open($fd, '<', '/etc/redis/redis.conf') or die("couldn't open redis config: $!");
	my @conf = <$fd>;
	close($fd);
	my @bind = grep {/^bind /} @conf;
	@bind or die('no "bind" config directive found');
	($server_ip) = $bind[0] =~ /^bind (\S+)/ or die("couldn't parse bind directive: $bind[0]");
	my @port = grep {/^port /} @conf;
	@port or die('no "port" config directive found');
	($port) = $port[0] =~ /^port (\d+)/ or die("couldn't parse port directive: $port[0]");
}

my $r = Redis->new(server => "$server_ip:$port", on_connect => sub {$_[0]->select(0)})
	or die("failed to create Redis object");
my $keys = $r->hkeys('sems_globals:cc_sw_prepaid') or die("failed to get keys for sems_globals:cc_sw_prepaid");

my $cleaned = 0;

for my $key (@{$keys}) {
	my $line = $r->hget('sems_globals:cc_sw_prepaid', $key) or next;
	my $toks = tokenize($line) or next;
	my $attrs = tokenize($toks->[3]) or next;
	my $xtime = $attrs->[3] or next;

	# check timeout
	time() - $xtime < ($max_days * 86400) and next;

	# check if call info is present
	my $info = $toks->[2];
	$r->hgetall($toks->[2]) or next;

	lprint("$key has a call since " . localtime($xtime) . " with info $info");
	my $ret = $r->hdel('sems_globals:cc_sw_prepaid', $key);
	lprint("$key has been cleaned, return code $ret");
	$cleaned++;
}

if ($cleaned) {
	lprint("cleaned $cleaned calls");
	if ($monit_restart) {
		system("monit restart sbc");
		lprint("sems restarted");
	}
}
else {
	lprint("nothing to clean");
}

exit;

sub tokenize {
	my ($line) = @_;
	my @ret;
	while ($line =~ s/^(\d+)://s) {
		my $num = $1;
		my $sub = substr($line, 0, $num, '');
		$line =~ s/^,//s or return;
		push(@ret, $sub);
	}
	return \@ret;
}

sub lprint {
	print(localtime() . " - " . $_[0] . "\n");
}
