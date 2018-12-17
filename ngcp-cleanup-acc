#!/usr/bin/perl

use strict;
use warnings;
use NGCP::Cleanup;

my $config_file = '/etc/ngcp-cleanup-tools/acc-cleanup.conf';

my $c;
eval {
    $c = new NGCP::Cleanup(config_file => $config_file);
    $c->process;
};
if ($@) {
    $c->error($@);
    $c->env('dbh')->disconnect if $c->env('dbh');
}

exit 0;
