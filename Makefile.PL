use ExtUtils::MakeMaker;

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

WriteMakefile
(
	($] ge '5.005') ?
	(
		'AUTHOR' => 'Ron Savage (ron@savage.net.au)',
		'ABSTRACT' => 'Convert an MS Access db to a MySQL/Postgres db',
    ) : (),
	clean				=>
	{
		FILES			=> 'blib/* Makefile MANIFEST DBIx-MSAccess-Convert2Db-*'
	},
	dist				=>
	{
		COMPRESS		=> 'gzip',
		SUFFIX			=> 'gz'
	},
	DISTNAME			=> 'DBIx-MSAccess-Convert2Db',
	NAME				=> 'DBIx::MSAccess::Convert2Db',
	PREREQ_PM			=> {Carp => 0, DBI => 0},
	VERSION_FROM		=> 'Convert2Db.pm',
);
