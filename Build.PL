use Module::Build;

Module::Build -> new
(
	module_name	=> 'DBIx::MSAccess::Convert2Db',
	license		=> 'artistic',
	dist_author	=> 'Ron Savage <ron@savage.net.au>',
	requires	=>
	{
		Carp			=> 0,
		DBI				=> 0,
		DBIx::SQLEngine	=> 0,
	},
) -> create_build_script();