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

var EventEmitter = require('events').EventEmitter;

const async = require('async'),
	  clc = require('cli-color'),
	  moment = require('moment'),
	  util = require('util'),
	  fs = require('fs'),
	  crypto = require('crypto'),
	  nodefs = require('node-fs'),
	  error = function(text) { util.log(clc.red(text)); process.exit(1) },
	  error_noshut = function(text) { util.log(clc.red(text)) },
	  warn = function(text) { util.log(clc.yellow(text)) };
	  success = function(text) { util.log(clc.green(text)) },
	  notice = function(text) { util.log(clc.cyanBright(text)) };

/*
 * Dump
 *
 * An object containing the functions to dump data to a file system or S3
 */
var Dump = {
	config: require('../config.json'),
	database: require('./database').database,
	e: new EventEmitter()
};

exports.dump = Dump;

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

			if (location.charAt(location.length - 1) == '/')
				location = location.substr(0, location.length - 2);
			// remove the last backslash

			if (!fs.existsSync(location))
				error(location + ' isnt a valid base path');
			// first we check if location is a valid path (do it sync just to save hassle)

			fs.mkdir(location + '/.test', function(err)
			{
				if (err)
					error('Cant create test folder, check your write permissions in ' + location);

				fs.rmdir(location + '/.test');
				// remove the directory

				callback();
				// move onto the next item
			});
		},
		function (callback)
		{
			fs.writeFile(location + '/.test-file', 'Checking write permissions...', function(err)
			{
				if (err)
					error('Cant create test file, check your write permissions in ' + location);

				fs.unlink(location + '/.test-file');
				// remove the test file

				callback();
				// move onto the next item
			});
		},
		function (callback)
		{
			success('Successfully ran permission tests');
			// we've successfully created a test folder and a test file. right on

			_this.location = location;
			_this.e.emit('ready');
			// once all this is done emit the complete callback so we can continue
		}
	]);
};

/*
 * Dump::save
 *
 * Dump the file structure object to the file system
 */
Dump.save = function(fileStructure)
{
	var _this = this,
		paths = wrote = 0,
		files = {};
		_this.metaData = [],
		_this.query = {'$or': []};

	async.series([
		function (callback)
		{
			notice('Preparing to dump data to file system...');
			notice('Setting up directory structures...');

			for (var account in fileStructure)
			{
				for (var network in fileStructure[account])
				{
					for (var target in fileStructure[account][network])
					{
						path = account + '/' + network + '/' + target;
						paths++;
						// build a path

						nodefs.mkdir(_this.location + '/' + path, 0777, true, function(err)
						{
							if (err)
								warn('WARNING: ' + err.replace(_this.location, ''));
							
							wrote++;
							
							if (paths == wrote)
								callback();
						});
						// make a call to nodefs to make the dir recursively, and not freak
						// if it already exists, we don't care if it exists, we just need it to
						// exist

						for (var file in fileStructure[account][network][target])
							files[_this.location + '/' + path + '/' + file] = fileStructure[account][network][target][file];
						// dump the files into a different object
					}
				}
			}
			// what we do here is create a directory string, such as
			// ricki/50d74c7006b3551ca8fd7812/#ircanywhere
			// we use the awesome node-fs module for this which will recursively
			// create these folders and not complain if they don't exist
		},
		function(callback)
		{
			success('Successfully setup directory structure');
			notice('Dumping data to file structure...');
			// once we're back here we know the structure has been setup

			_this.attemptWrite(files, 1, 5, function()
			{
				success('Successfully saved ' + Object.keys(files).length + ' file(s)');
				// some notices

				_this.database.finish(function() {
					_this.e.emit('finished');
				});
				// attempt to write meta data
			},
			function(ret)
			{
				error_noshut('WARNING: Retry attempts failed, not saving ' + Object.keys(ret).length + ' file(s)');
				for (var i in ret)
					error_noshut('         ' + i.replace(_this.location, ''));
				
				warn('Will not remove these records from the database');

				callback();
			});
		}
	]);
};

/*
 * Dump::attemptWrite
 *
 * Attempt to write data, recursively attempt to write failed
 * attempts up to x times specified in the retry parameter
 *
 * Parameters: attempt (integer) number of attempts
 *             retry (integer) how many times to retry
 *             complete (function) on success callback
 *             failure (function) on failure callback
 */
Dump.attemptWrite = function(files, attempt, retry, complete, failure)
{
	var _this = this;
	
	_this.e.once('write finished', function(ret)
	{
		if (attempt > retry)
		{
			failure(ret);
			return;
		}
		// failed to complete, send the files array to the function

		if (ret === true)
		{
			complete();
			return;
		}
		// completed
		else if (typeof ret === 'object' && Object.keys(ret).length > 0)
		{
			warn('WARNING: Failed to write ' + Object.keys(ret).length + ' file(s)');
			for (var i in ret)
				warn('         ' + i.replace(_this.location, ''));

			warn('Will attempt to write again in 5 seconds');

			var counter = 5,
				countdown = setInterval(function()
				{
					if (counter == -1)
					{
						notice('Dumping remaining data to file structure, attempt ' + (attempt + 1) + '...')
						_this.attemptWrite(ret, attempt + 1, retry, complete, failure);
						return clearInterval(countdown);
					}

					process.stdout.write(clc.bol(-1));
					warn('Will attempt to write again in ' + counter + ' seconds');
					counter--;

				}, 1000);
		}
		// output that x document couldnt be written
	});

	_this.writeFileObject(files);
};

/*
 * Dump::writeFileObject
 *
 * A function which writes a file object to disk, doesnt do any checking
 *
 * Returns true if complete or an array of documents that were not written if not
 */
Dump.writeFileObject = function(files)
{
	var _this = this,
		keys = start = Object.keys(files),
		tens = Math.floor(keys.length / 10),
		percent = c = i = docs = 0,
		done = [],
		returnObj = {};

	async.forEachSeries(keys, function(item, callback)
	{
		docs += files[item].length;
		var dump = unescape(encodeURIComponent(JSON.stringify(files[item]))),
			location = item.replace(_this.location, ''),
			data = location.split('/'),
			s3hash = crypto.createHash('md5').update(location).digest('hex');
		
		fs.writeFile(item, dump, 0777, function(err)
		{
			if (!err)
			{
				done.push(item);

				if (c == tens && percent < 100)
				{
					c = 0;
					percent += 10;
					notice(_this.database.partitionData.collection.name + ':   ... Partitioned ' + docs + ' documents into ' + i + ' file(s) (' + percent + '%)');
				}
				// are we on a percentage? notify the end user

				_this.metaData.push({
					account: data[1],
					network: data[2],
					target: data[3],
					date: data[4],
					timestamp: +new Date(),
					baseLocation: _this.location,
					location: location,
					s3hash: s3hash
				});
				// input this into the meta data object

				var startOfDay = moment.utc(data[4], 'DD-MM-YYYY').startOf('day'),
					endOfDay = moment.utc(data[4], 'DD-MM-YYYY').endOf('day'),
					queryObj = {
						account: data[1],
						network: data[2],
						timestamp: {'$gt': Date.parse(startOfDay._d), '$lt': Date.parse(endOfDay._d)}
					};
				// construct a query object

				if (data[3] == 'status')
					queryObj.status = true;
				else
					queryObj.target = data[3];
				// target or status????

				_this.query['$or'].push(queryObj);
				// push the object to our query
			}

			i++;
			c++;
			// up some variables

			callback();
			// next item
		});
		// attempted to dump all the files in order
	},
	function(err)
	{
		if (percent != 100)
			notice(_this.database.partitionData.collection.name + ':   ... Partitioned ' + docs + ' documents into ' + keys.length + ' file(s) (100%)');
		// we've finished, notice it

		var check = '',
			failed = false;

		for (var i = 0; i < start.length; i++)
		{
			check = start[i];
			if (done.indexOf(check) == -1)
			{
				failed = true;
				returnObj[check] = files[check];
			}
		}
		// find documents that were not written

		var ret = (!failed) ? true : returnObj;

		_this.e.emit('write finished', ret);
		// finished attempting to write
	})
};

/*
 * Dump::dumpMetaData
 *
 * A function which dumps the meta data to disk if database dump fails
 */
Dump.dumpMetaData = function()
{
	var _this = this,
		date = moment.utc().format('DD-MM-YYYY'),
		file = _this.location + '/meta_data.' + date;

	notice('Attempting to write meta data to disk...');
	fs.writeFile(file, JSON.stringify(_this.metaData), 0777, function(err)
	{
		if (err)
			error('Failed to dump meta data to disk, shutting down');
		
		success('Successfully dumped meta data to ' + file);
	});
};