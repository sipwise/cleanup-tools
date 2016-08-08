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

for my $p (@{$cfg{paths}}) {
	my $maxage = $p->{max_age_days} // $cfg{max_age_days};
	$maxage or next;
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
		print("directory: $path\n");
	}
	elsif (-f $path) {
		print("file: $path\n");
	}
}
