#!/usr/bin/perl
#
# Name:
#	msaccess2db.pl.
#
# Purpose:
#	Read an MS Access database and write a real RBDMS database.
#
# Author:
#	Ron Savage <ron@savage.net.au>

use strict;
use warnings;

use Error qw/:try/;
use DBIx::MSAccess::Convert2Db;

# -----------------------------------------------

my($mids) = DBIx::MSAccess::Convert2Db -> new
(
	access_dsn		=> 'mnt',
	db_username		=> ($^O eq 'MSWin32') ? 'root' : 'postgres',
	db_password		=> ($^O eq 'MSWin32') ? 'pass' : '',
	db_name			=> 'mnt',
	driver			=> ($^O eq 'MSWin32') ? 'mysql' : 'Pg',
	lower_case		=> 1,
	null_to_blank	=> 1,
	verbose			=> 1,
);

try
{
	$mids -> do('drop database mnt');
	$mids -> do('create database mnt');

	my($table_name) = $mids -> get_access_table_names(['Access_levels', 'Titles']);

	$mids -> convert($_) for @$table_name;
}
catch Error::Simple with
{
	my($error) = $_[0] -> text();
	chomp($error);
	print "Error: $error. \n";
};
