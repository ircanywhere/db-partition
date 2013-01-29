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
	  clc = require('cli-color'),
	  util = require('util'),
	  fs = require('fs'),
	  error = function(text) { util.log(clc.red(text)); process.exit(1) },
	  success = function(text) { util.log(clc.green(text)) },
	  notice = function(text) { util.log(clc.yellow(text)) };

/*
 * Dump
 *
 * An object containing the functions to dump data to a file system or S3
 */
var Dump = {
	config: require('../config.json'),
	database: require('./database').database
};

/*
 * Dump::setup
 *
 * Attempt to make sure the file system / S3 bucket is ready for use
 */
Dump.setup = function()
{
	var _this = this,
		backupto = _this.config.backupto,
		fsconf = _this.config.fs,
		location = '';
	
	async.series([
		function (callback)
		{
			if (backupto == undefined || backupto != 'fs')
				error('Invalid value in config.backupto, valid values are \'fs\'');
			// no backup method

			if (fsconf == undefined || typeof fsconf != 'object')
				error('You have not provided config.fs');
			// invalid backup conf object

			location = fsconf.location;
			
			if (location == undefined || typeof location != 'string')
				error('You have not provided config.fs.location');
			// no backup conf location

			callback();
			// config is done checking, move on
		},
		function (callback)
		{
			notice('Using filesystem... checking file system paths and permissions...')

			if (!fs.existsSync(location))
				error(location + ' isnt a valid base path, shutting down');
			// first we check if location is a valid path (do it sync just to save hassle)
		}
	]);
};

exports.dump = Dump;