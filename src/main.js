/*
 * db-partition
 * 
 * A small program which works out how many buffers there is in
 * the buffer table, cuts the most recent x days out. Takes that
 * stores it in files and adds a row into a collection with info
 * on how to retreive it and removes original records from buffers
 * collection.
 *
 * Designed to keep the database size relatively small and keep the
 * old backlogs in somewhere like Amazon's S3 or a file structure
 *
 * Copyright (c) 2013 IRCAnywhere <support@ircanywhere.com>.
 * Rights to this code are as documented in LICENSE.
 */

const async = require('async'),
	  pjson = require('../package.json'),
	  settings = require('../settings.js').settings,
	  config = require('../settings.js').config,
	  clc = require('cli-color'),
	  database = require('./database').database,
	  dump = require('./dump').dump,
	  util = require('util'),
	  error = function(text) { util.log(clc.red(text)); process.exit(1) },
	  warn = function(text) { util.log(clc.yellow(text)) };
	  success = function(text) { util.log(clc.green(text)) },
	  notice = function(text) { util.log(clc.cyanBright(text)) };

/*
 * main
 *
 * Like a C program, first thing that gets executed
 */
function main()
{
	console.log(' ');
	console.log('      8 8                         w   w  w   w');
	console.log('   .d88 88b.      88b. .d88 8d8b w8ww w w8ww w .d8b. 8d8b. .d88b 8d8b');
	console.log('   8  8 8  8 wwww 8  8 8  8 8P    8   8  8   8 8\' .8 8P Y8 8.dP\' 8P');
	console.log('   `Y88 88P\'      88P\' `Y88 8     Y8P 8  Y8P 8 `Y8P\' 8   8 `Y88P 8');
	console.log('                  8');
	console.log(' ');
	console.log('    v ' + pjson.version + ' Copyright (c) 2013 IRCAnywhere <support@ircanywhere.com>');
	console.log(' ');

	database.setup();
	// make sure the database is set up and completely ready before we do anything else
	
	database.e.on('ready', function() {
		dump.setup();
	});
	// once the database has complete setting up, setup the file system

	dump.e.on('ready', function() {
		database.analyse();
	});
	// dump system has completed setting up and ran tests

	database.e.on('finished analysing', function()
	{
		if (process.argv.indexOf('--analyse') == -1)
			database.partition();
		else
			process.exit(1);
		// if --analyse isnt specified then go ahead and partition
	});
	// finished analysing

	database.e.on('finished organising', function(fileStructure)
	{
		if (config.backupto == 'fs')
			dump.save(fileStructure);
	});
	// finished organising

	dump.e.on('finished', function()
	{
		database.conn.close();
		process.exit(1);
	});
	// finished dumping
};

main();