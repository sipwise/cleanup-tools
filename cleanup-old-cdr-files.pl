#!/usr/bin/perl

use strict;
use warnings;
use Config::Any;
use File::Find;

my $cfgs = Config::Any->load_files({files => ['/etc/ngcp-cleanup-tools/cdr-files-cleanup.yml'],
		use_ext => 1, flatten_to_hash => 1});
if (!$cfgs || !values(%{$cfgs})) {
	die("no config found");
}
my %cfg = map {%{$_}} values(%{$cfgs});

$cfg{enabled} =~ /1|yes/ or exit(0);

my $now = time();
my $maxage;

for my $p (@{$cfg{paths}}) {
	$maxage = $p->{max_age_days} // $cfg{max_age_days};
	$maxage or next;
	$maxage *= 86400;
	my @paths = ($p->{path});
	if ($p->{wildcard} =~ /1|yes/) {
		@paths = glob($p->{path});
	}
	for my $path (@paths) {
		-d $path or next;
		finddepth(\&recurser, $path);
	}
}

sub recurser {
	my $path = $File::Find::name;
	if (-d $path) {
		#print("directory: $path\n");
		rmdir($path); # ignore errors
	}
	elsif (-f $path) {
		#print("file: $path\n");
		my @sb = stat($path);
		@sb or return;
		my $age = $now - $sb[9];
		$age < $maxage and return;
		#print("delete: $path\n");
		unlink($path);
	}
}
