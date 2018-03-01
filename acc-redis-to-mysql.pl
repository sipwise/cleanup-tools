#!/usr/bin/perl
use strict;
use warnings;

use Redis;

my $config_redis_url = "127.0.0.1:6379";
my $config_redis_db = 21;
my $threshold = 60 * 24 * 3600; # backup everything older than $threshold seconds

my $scan_key = "acc:entry::*"; # the redis "table" name
my $scan_count = 1000; # how many entries to fetch per scan loop

# make sure to have method as first element, as we rely on it later down
my @cols = qw/
	method from_tag to_tag callid sip_code sip_reason
	time time_hires src_leg dst_leg dst_user dst_ouser
	dst_domain src_user src_domain
/;

my $redis = Redis->new(server => $config_redis_url)
	or die "Failed to connect to redis at '$config_redis_url'\n";
$redis->select($config_redis_db);

my $cursor = 0;
my $now = time;

# TODO: derive table name from something
my $table = "kamailio.acc_201801";

# TODO: execute create table statement, like:
# $dbh->do("CREATE TABLE IF NOT EXISTS kamailio.$table LIKE kamailio.acc");

do {
	my $res = $redis->scan($cursor, MATCH => $scan_key, COUNT => $scan_count);
	$cursor = shift @{ $res };
	my $keys = shift @{ $res };

	my $query = "INSERT INTO $table (" . join(',', @cols) . ") VALUES ";

	if (@{ $keys }) {
		my %vals = ();
		foreach my $key (@{ $keys }) {
			my @parts = split /::?/, $key;
			my $time = int($parts[3]);


			next if (($time + $threshold) > $now);

			my $data = $redis->hmget($key, @cols);
			$query .= "('" . join("','", @{ $data }) . "'),";

			$vals{$key} = {
				meth => $data->[0],
				cid  => $parts[2],
			};
		}
		$query =~ s/,$//;

		# TODO: execute query, abort if it fails to not delete
		# uninserted entries from redis
		# $dbh->do($query) or die "Failed to insert redis acc entries to mysql\n";
		
		foreach my $key (@{ $keys }) {
			# delete direct entry
			$redis->del($key);

			my $cid = $vals{$key}->{cid};
			my $meth = $vals{$key}->{meth};

			# delete from cid map
			$redis->srem("acc:cid::$cid", $key);
			# delete from meth map
			$redis->srem("acc:meth::$meth", $key);
		}
	}
} while($cursor);
