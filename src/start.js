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

const clc = require('cli-color'),
	  pjson = require('../package.json');

/*
 * Partition
 *
 * A partition object containing everything
 */
var Partition = {
	config: require('../config.json'),
	database: require('./database').database
};

/*
 * Partition::start
 *
 * This is the first place we go to after the database has connected
 */
Partition.start = function()
{
	var _this = this;

	util.log('hi');
};

/*
 * main
 *
 * Like a C program, first thing that gets executed
 */
function main(args)
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

	Partition.database.setup();
	// this function sends us back to .start() once connected 
}

exports.partition = Partition;
main(process.argv);