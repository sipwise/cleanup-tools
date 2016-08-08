#!/usr/bin/perl

use strict;
use warnings;
use Config::Any;

my $cfgs = Config::Any->load_files({files => ['/etc/ngcp-cleanup-tools/cdr-files-cleanup.yml'],
		use_ext => 1, flatten_to_hash => 1});
if (!$cfgs || !values(%{$cfgs})) {
	die("no config found");
}
my %cfg = map {%{$_}} values(%{$cfgs});
