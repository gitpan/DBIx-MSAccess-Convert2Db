package DBIx::MSAccess::Convert2Db;

# Documentation:
#	POD-style documentation is at the end. Extract it with pod2html.*.
#
# Note:
#	o tab = 4 spaces || die
#
# Author:
#	Ron Savage <ron@savage.net.au>

use strict;
use warnings;

require 5.005_62;

require Exporter;

use Carp;
use DBI;
use DBIx::SQLEngine;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use DBIx::MSAccess::Convert2Db ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(

) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(

);
our $VERSION = '1.01';

# -----------------------------------------------

# Preloaded methods go here.

# -----------------------------------------------

# Encapsulated class data.

{
	my(%_attr_data) =
	(	# Alphabetical order.
		_access_dsn			=> '',
		_access_password	=> '',
		_access_username	=> '',
		_db_name			=> '',
		_db_password		=> '',
		_db_username		=> '',
		_driver				=> '',
		_lower_case			=> 0,
		_null_to_blank		=> 0,
		_verbose			=> 0,
	);

	sub _default_for
	{
		my($self, $attr_name) = @_;

		$_attr_data{$attr_name};
	}

	sub _standard_keys
	{
		sort keys %_attr_data;
	}

}	# End of Encapsulated class data.

# -----------------------------------------------

sub convert
{
	my($self, $table_name)	= @_;
	my($column_name)		= $self -> get_access_column_names($table_name);
	my(@lc_column_name)		= map{$_ =~ tr/ /_/s; $$self{'_lower_case'} ? lc $_ : $_} @$column_name;
	my $data				= $$self{'_access_engine'} -> fetch_select(table => "$$self{'_quote'}$table_name$$self{'_quote'}");
	my(%invalid)			=
	(
		order => '_order',
		ORDER => '_ORDER',
	);
	my($column)			= [map{ {name => ($invalid{$_} ? $invalid{$_} : $_), type => 'varchar(255)'} } @lc_column_name];
	my($new_table_name)	= $$self{'_lower_case'} ? lc $table_name : $table_name;
	$new_table_name		=~ tr/ /_/s;
	my(@dummy)			= $$self{'_db_engine'} -> detect_table($new_table_name, 1);
	my($count)			= 0;

	$self -> log("Table: $new_table_name. Columns:");
	$self -> log($_) for map{$count++; "$count: $_"} @lc_column_name;
	$self -> log();

	if ($#dummy >= 0)
	{
		$$self{'_db_engine'} -> do_drop_table($new_table_name);
		$self -> log("Dropped table: $new_table_name");
	}

	$$self{'_db_engine'} -> do_create_table($new_table_name, $column);
	$self -> log("Created table: $new_table_name");

	for my $row (@$data)
	{
		$$self{'_db_engine'} -> do_insert
		(
			table	=> $new_table_name,
			columns	=> [map{$invalid{$_} ? $invalid{$_} : $_} @lc_column_name],
			values	=> [map{$$row{$_} ? $$row{$_} : $$self{'_null_to_blank'} ? '' : undef} @$column_name],
		);
	}

}	# End of convert.

# -----------------------------------------------

sub DESTROY
{
	my($self) = @_;

	$self -> save_log() if ($$self{'_verbose'});

}	# End of DESTROY.

# -----------------------------------------------

sub do
{
	my($self, $sql) = @_;

	$$self{'_db_engine'} -> do_sql($sql);

	$self -> log("Executed SQL: $sql");

}	# End of do.

# -----------------------------------------------

sub do_create_index
{
	my($self, $table_name, $index_column, $unique) = @_;
	my($sql) = 'create ' . ($unique ? 'unique ' : '') . "index ${index_column}_index on $table_name ($index_column)";

	$$self{'_db_engine'} -> do_sql($sql);

	$self -> log("Executed SQL: $sql");

}	# End of do_create_index.

# -----------------------------------------------

sub do_create_table
{
	my($self, $table_name, $column) = @_;

	$$self{'_db_engine'} -> do_create_table($table_name, $column);
	$self -> log("Created table: $table_name");

	# If Pg, then a sequence is created implicitly for type = sequential.
	# In which case, we must explicitly create the index for the primary key.

	if ($$self{'_driver'} eq 'Pg')
	{
		my($col);

		for $col (@$column)
		{
			next if ($$col{'type'} ne 'sequential');

			# Name uses $table_$column for both implicit sequence and explicit index.

			my($name)	= "${table_name}_$$col{'name'}";
			my($sql)	= "create unique index ${name}_index on $table_name ($$col{'name'})";

			$$self{'_db_engine'} -> do_sql($sql);
			$self -> log("Executed SQL: $sql");

			push @{$$self{'_index'} }, $name;
		}
	}

}	# End of do_create_table.

# -----------------------------------------------

sub do_drop_table
{
	my($self, $table_name)	= @_;
	my(@column)				= $$self{'_db_engine'} -> detect_table($table_name, 1);

	if ($#column >= 0)
	{
		$$self{'_db_engine'} -> do_drop_table($table_name);
		$self -> log("Dropped table: $table_name");

		# If Pg, then primary index is created explicitly.
		# In which case, we must explicitly drop the index.
		# And, we must explicitly drop the sequence for the primary key.

		if ($$self{'_driver'} eq 'Pg')
		{
			my($name, $sql);

			for $name (@{$$self{'_index'} })
			{
				$sql = "drop index ${name}_index";

				$$self{'_db_engine'} -> do_sql($sql);
				$self -> log("Executed SQL: $sql");

				$sql = "drop sequence ${name}_seq";

				$$self{'_db_engine'} -> do_sql($sql);
				$self -> log("Executed SQL: $sql");
			}
		}
	}

}	# End of do_drop_table.

# -----------------------------------------------

sub get_access_column_names
{
	my($self, $table_name)	= @_;
	my $data				= $$self{'_access_engine'} -> fetch_one_row(table => "$$self{'_quote'}$table_name$$self{'_quote'}");

	[sort keys %$data];

}	# End of get_access_column_names.

# -----------------------------------------------

sub get_access_table_names
{
	my($self, $want)	= @_;
	my(@table_name)		= sort $$self{'_access_dbh'} -> tables();
	my($count)			= 0;

	$self -> log('Table names - raw list:');
	$self -> log($_) for map{$count++; "$count: $_"} @table_name;
	$self -> log();

	@table_name	= map{s/^$$self{'_quote'}.+?$$self{'_quote'}\.$$self{'_quote'}(.+)$$self{'_quote'}/$1/; $1} @table_name;
	$count		= 0;

	$self -> log('Table names - clean list:');
	$self -> log($_) for map{$count++; "$count: $_"} @table_name;
	$self -> log();

	if ($want && ($#$want >= 0) )
	{
		my(%want);

		@$want			= map{lc} @$want;
		@want{@$want}	= (1) x @$want;
		@table_name		= grep{$_} map{my($name) = lc; $want{$name} ? $_ : 0} @table_name;
		$count			= 0;

		$self -> log('Table names - wanted list:');
		$self -> log($_) for map{$count++; "$count: $_"} @table_name;
		$self -> log();
	}

	$$self{'_table'} = \@table_name;

}	# End of get_access_table_names.

# -----------------------------------------------

sub log
{
	my($self, $s)	= @_;
	$s				= '' if (! $s);

	push @{$$self{'_log'} }, $s;

}	# End of log.

# -----------------------------------------------

sub new
{
	my($caller, %arg)		= @_;
	my($caller_is_obj)		= ref($caller);
	my($class)				= $caller_is_obj || $caller;
	my($self)				= bless({}, $class);

	for my $attr_name ($self -> _standard_keys() )
	{
		my($arg_name) = $attr_name =~ /^_(.*)/;

		if (exists($arg{$arg_name}) )
		{
			$$self{$attr_name} = $arg{$arg_name};
		}
		elsif ($caller_is_obj)
		{
			$$self{$attr_name} = $$caller{$attr_name};
		}
		else
		{
			$$self{$attr_name} = $self -> _default_for($attr_name);
		}
	}

	Carp::croak("No value specified for parameter 'access_dsn'")	if (! $$self{'_access_dsn'});
	Carp::croak("No value specified for parameter 'db_name'")		if (! $$self{'_db_name'});
	Carp::croak("No value specified for parameter 'driver'")		if (! $$self{'_driver'});

	$$self{'_access_engine'} = DBIx::SQLEngine -> new
	(
		"dbi:ODBC:$$self{'_access_dsn'}", $$self{'_access_username'}, $$self{'_access_password'},
		{
			AutoCommit			=> 1,
			PrintError			=> 0,
			RaiseError			=> 1,
			ShowErrorStatement	=> 1,
		}
	);
	$$self{'_access_dbh'}	= $$self{'_access_engine'} -> get_dbh();
	$$self{'_db_engine'}	= DBIx::SQLEngine -> new
	(
		"dbi:$$self{'_driver'}:$$self{'_db_name'}", $$self{'_db_username'}, $$self{'_db_password'},
		{
			AutoCommit			=> 1,
			PrintError			=> 0,
			RaiseError			=> 1,
			ShowErrorStatement	=> 1,
		}
	);
	$$self{'_db_dbh'}			= $$self{'_db_engine'} -> get_dbh();
	$$self{'_index'}			= [];
	$$self{'_log'}				= [];
	$$self{'_quote'}			= $$self{'_access_dbh'} -> get_info(29) || ''; # SQL_IDENTIFIER_QUOTE_CHAR.
	$$self{'_table'}			= [];
	$$self{'_temp'}				= ($^O eq 'MSWin32') ? 'temp' : 'tmp';
	$$self{'_log_file_name'}	= "/$$self{'_temp'}/msaccess2db.log";

	$self -> log('Open log: ' . scalar localtime() );
	$self -> log("Converting MS Access file via DSN '$$self{'_access_dsn'}' to $$self{'_driver'} database '$$self{'_db_name'}'");

	$self;

}	# End of new.

# -----------------------------------------------

sub save_log
{
	my($self, $log_file_name)	= @_;
	$$self{'_log_file_name'}	= $log_file_name if ($log_file_name);

	unlink $$self{'_log_file_name'};

	(-e $$self{'_log_file_name'}) && throw Error::Simple("Can't unlink $$self{'_log_file_name'}): $!");

	open(OUT, "> $$self{'_log_file_name'}") || throw Error::Simple("Can't open(> $$self{'_log_file_name'}): $!");
	print OUT map{"$_\n"} @{$$self{'_log'} };
	print OUT "Close log: ", scalar localtime(), ". \n";
	close OUT;

	$$self{'_log'} = [];

}	# End of save_log.

# -----------------------------------------------

1;

__END__

=head1 NAME

C<DBIx::MSAccess:Convert2Db> - Convert an MS Access database into a MySQL/Postgres/Other database

=head1 Synopsis

	use DBIx::MSAccess:Convert2Db;

	my($obj) = DBIx::MSAccess:Convert2Db -> new
	(
	    access_dsn    => 'in',
	    db_username   => ($^O eq 'MSWin32') ? 'root' : 'postgres',
	    db_password   => ($^O eq 'MSWin32') ? 'pass' : '',
	    db_name       => 'out',
	    driver        => ($^O eq 'MSWin32') ? 'mysql' : 'Pg',
	    lower_case    => 1,
	    null_to_blank => 1,
	    verbose       => 1,
	);

	$obj -> do('drop database out');
	$obj -> do('create database out');

	my($table_name) = $obj -> get_access_table_names(['table a', 'table b']);

	$obj -> convert($_) for @$table_name;

=head1 Description

C<DBIx::MSAccess:Convert2Db> is a pure Perl module.

It can convert an MS Access database into one in MySQL/Postgres/Other format.

The conversion is mindless. In particular, this version does not even use the Date::MSAccess module to convert
dates.

Hopefully, this means the output database is an exact copy of the input one, apart from perhaps some column truncation.

Things to note:

=over 4

=item The module uses DBIx::SQLEngine to achieve a degree of database vendor-independence

=item The module uses DBD::ODBC to connect via a DSN to MS Access

See below for more on this DSN (Data Source Name). Search down for 'access_dsn'.

=item All candidate output table names are obtained from the MS Access database

You can have the module ignore input tables or views by passing to get_access_table_names() an array ref
of the names of those tables you wish to output.

	my($table_name) = $obj -> get_access_table_names();

returns an array ref of all table names in the MS Access database, so all table names will be passed to convert().

	my($table_name) = $obj -> get_access_table_names(['table a', 'table b']);

returns an array ref of table names to be passed to convert(), with tables called 'table a' and 'table b'
being the only ones included in the list.

=item All output table names can be converted to lower case

Use the option new(lower_case => 1) to activate this action.

=item All output table names have ' ' characters in their names replaced by '_'

=item All output column names are from the MS Access database

=item All output column names have MySQL/Postgres reserved words prefixed with '_'

That is, $original_column_name is replaced by "_$original_column_name".

The only known case (20-Jan-2004) is any column named 'Order', which will be called '_order' in the output database.

=item All output column names can be converted to lower case

Use the option new(lower_case => 1) to activate this action.

=item All output columns are of type varchar(255)

Note: This will cause data to be truncated if input columns are longer than 255 characters.

=item This module has only been tested under MS Windows and MySQL

It does contain, I believe, all the code required to run under Postgres. However, I have never tried to use a DSN
under Unix, so YMMV.

=back

=head1 Distributions

This module is available both as a Unix-style distro (*.tgz) and an
ActiveState-style distro (*.ppd). The latter is shipped in a *.zip file.

See http://savage.net.au/Perl-modules.html for details.

See http://savage.net.au/Perl-modules/html/installing-a-module.html for
help on unpacking and installing each type of distro.

=head1 Constructor and initialization

new(...) returns a C<DBIx::MSAccess:Convert2Db> object.

This is the class's contructor.

Usage: DBIx::MSAccess:Convert2Db -> new().

This option takes a set of options.

=over 4

=item access_dsn

The DSN (Data Source Name) of the MS Access database.

To start creating a DSN under Win2K, say, go to Start/Settings/Control Panel/Admin tools/Data Source (ODBC)/System DSN.

Note: A System DSN is preferred because it is visible to all users, not just the currently logged in user.

This option is mandatory.

=item db_username

The user name to use to log in to the output database.

This might be something like

	($^O eq 'MSWin32') ? 'root' : 'postgres'

if you are using MySQL under Windows and Postgres under Unix.

The default is the empty string.

=item db_password

The password to use to log in to the output database.

This might be something like

	($^O eq 'MSWin32') ? 'pass' : ''

if you are using MySQL under Windows and Postgres under Unix.

The default is the empty string.

=item db_name

The output database name.

This option is mandatory.

=item driver

The output database driver.

This might be something like

	($^O eq 'MSWin32') ? 'mysql' : 'Pg'

This option is mandatory.

=item lower_case

An option, either 0 or 1, to activate the conversion of all table names and column names to lower case, in the
output database.

The default is 0.

=item null_to_blank

An option, either 0 or 1, to activate the conversion of all null values to the empty string, in the output database.

The default is 0.

=item verbose

An option, either 0 or 1, to activate the writing to disk of various bits of information.

The default is 0.

The output disk file name is determined by this code:

	$$self{'_temp'}          = ($^O eq 'MSWin32') ? 'temp' : 'tmp';
	$$self{'_log_file_name'} = "/$$self{'_temp'}/msaccess2db.log";

=back

=head1 Method: get_access_table_names([An array ref of table names to output])

Returns an array ref of table name to be passed to convert().

=head1 Method: convert($table_name)

Returns nothing.

Converts one table from MS Access format to MySQL/Postgres/Other format.

It's normally called like this:

	my($table_name) = $obj -> get_access_table_names();

	$obj -> convert($_) for @$table_name;

=head1 Example code

See the examples/ directory in the distro.

Note: The example uses a module called Error.

Note: Activestate-style distros do not contain this directory :-(.

=head1 Required Modules

Carp, DBI, DBD::ODBD, DBIx::SQLEngine.

=head1 Changes

See Changes.txt.

=head1 Author

C<DBIx::MSAccess:Convert2Db> was written by Ron Savage I<E<lt>ron@savage.net.auE<gt>> in 2004.

Home page: http://savage.net.au/index.html

=head1 Copyright

Australian copyright (c) 2004, Ron Savage. All rights reserved.

	All Programs of mine are 'OSI Certified Open Source Software';
	you can redistribute them and/or modify them under the terms of
	The Artistic License, a copy of which is available at:
	http://www.opensource.org/licenses/index.html

=cut
