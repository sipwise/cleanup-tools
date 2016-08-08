#!/usr/bin/perl

use strict;
use warnings;
use Config::Any;

my $cfgs = Config::Any->load_files({files => ['/etc/ngcp-cleanup-tools/cdr-files-cleanup.yml'], use_ext => 1});
if (!$cfgs || !@{$cfgs}) {
	die("no config found");
}

