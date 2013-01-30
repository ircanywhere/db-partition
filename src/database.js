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
	  moment = require('moment'),
	  clc = require('cli-color'),
	  util = require('util'),
	  error = function(text) { util.log(clc.red(text)); process.exit(1) },
	  error_noshut = function(text) { util.log(clc.red(text)) },
	  warn = function(text) { util.log(clc.yellow(text)) };
	  success = function(text) { util.log(clc.green(text)) },
	  notice = function(text) { util.log(clc.cyanBright(text)) };

/*
 * Database
 *
 * A database object which contains all settings and handles connections and lookups
 */
var Database = {
	config: require('../config.json'),
	mongoose: require('mongoose'),
	e: new EventEmitter()
};

exports.database = Database;

/*
 * Database::setup
 *
 * Setup the database and perform some tests
 */
Database.setup = function()
{
	var _this = this,
		ObjectId = _this.mongoose.Schema.ObjectId,
		databaseUrl = _this.config.url,
		database = _this.config.database,
		collection = _this.config.collection || 'buffers',
		metaCollection = _this.config.metaCollection || 'buffersMeta';

	if (databaseUrl == undefined)
		error('You have not provided a url or database to connect to');
	// no mongodb url, bail

	_this.conn = _this.mongoose.createConnection(databaseUrl + '/' + database);

	_this.conn.on('error', function(err) {
		error(err);
	});
	// couldn't connect

	_this.conn.on('open', function()
	{
		async.series([
			function (callback)
			{
				success('Sucessfully connected to the mongodb database');
				notice('Checking if collections exist...');

				_this.conn.db.collectionNames(function(err, collections)
				{
					var partitionCollection = database + '.' + collection,
						metaDataCollection = database + '.' + metaCollection,
						pExists = false,
						mdExists = false;
					
					for (var cid in collections)
					{
						var coll = collections[cid];

						if (coll.name == partitionCollection)
							pExists = true;
						if (coll.name == metaDataCollection)
							mdExists = true;
					}

					if (pExists)
						success('Partition collection:   ' + partitionCollection + ' exists');
					else
						error('Partition collection:   ' + partitionCollection + ' doesn\'t exist, nothing to partition');
					// check if our partition collection exists

					if (mdExists)
					{
						success('Metadata collection:    ' + metaDataCollection + ' exists');
					}
					else
					{
						notice('Metadata collection:    ' + metaDataCollection + ' doesn\'t exist, creating collection');
						_this.conn.db.createCollection(metaCollection, {capped: false}, function(err, coll) {
							success('Metadata collection:    successfully created ' + metaDataCollection);
							callback();
						});
					}	
					// check if our metadata collection exists
					// not fatal if it doesn't, we can just create it.

					callback();
				});
				// check if our collection exists
			},
			function (callback)
			{
				var PartitionModel = new _this.mongoose.Schema({
					account: String,
					network: ObjectId,
					timestamp: Number,
					read: Boolean,
					nick: String,
					target: String,
					self: Boolean,
					status: Boolean,
					privmsg: Boolean,
					json: {}
				});

				var MetaDataModel = new _this.mongoose.Schema({
					account: String,
					network: ObjectId,
					target: String,
					date: String,
					timestamp: Number,
					baseLocation: String,
					location: String,
					s3hash: String
				});

				_this.partitionData = _this.conn.model(collection, PartitionModel, collection);
				_this.metaData = _this.conn.model(metaCollection, MetaDataModel, metaCollection);
				// setup the schema

				notice('Partition collection:   setting up schema');
				notice('Metadata collection:    setting up schema');
				// setup our models and schemas here

				callback();
			},
			function(callback)
			{
				_this.e.emit('ready');
				// once all this is done emit the complete callback so we can continue
			}
		]);
		// execute tasks in order
	});
	// connected, move on.
};

/*
 * Database::analyse
 *
 * Analyse our structure and determine how many results we've got etc
 */
Database.analyse = function()
{
	var _this = this,
		days = _this.config.olderthan || 28;
	// _this so we don't have to bind
	// days defaults to 28 (4 week) if undefined

	var from = moment.utc().subtract('days', days).startOf('day');
	// thank fuck for moment, probably cut 10 lines down to one piss easy call

	_this.partitionQuery = {timestamp: {'$lt': Date.parse(from._d)	}};
	// lets first setup the query that grabs the last x days worth of data

	notice('Analysing data structures...');
	
	async.waterfall([
		function (callback)
		{
			_this.partitionData.count(function(err, count)
			{
				notice(_this.partitionData.collection.name + ':   ' + count + ' documents found...');
				callback(null, count);
			});
		},
		function (count, callback)
		{
			_this.partitionData.count(_this.partitionQuery, function(err, pcount)
			{
				var percentage = pcount / count * 100; 
				notice(_this.partitionData.collection.name + ':   ' + pcount + ' documents will be partitioned (' + percentage.toFixed(2) + '%)...');
				callback(null, count, pcount);
			});
		},
		function (count, pcount, callback)
		{
			_this.count = count;
			_this.pcount = pcount;
			_this.e.emit('finished analysing');
			// once all this is done emit the complete callback so we can continue
		}
	]);
	// loving async, execute these in a series, without embedding callbacks
};

/*
 * Database::partition
 *
 * Start to "partition" our data, ie organising it into a structure
 * and figure out where to dump it all etc.
 */
Database.partition = function()
{
	var _this = this,
		fileStructure = {};

	notice('Organising data ready for partitioning...');

	async.waterfall([
		function (callback)
		{
			var stream = _this.partitionData.find(_this.partitionQuery).sort({timestamp: 'asc'}).stream(),
				tens = Math.floor(_this.pcount / 10),
				percent = c = i = 0;
			// setup a stream and make some calculations

			stream.on('data', function(record)
			{
				if (c == tens && percent < 100)
				{
					c = 0;
					percent += 10;
					notice(_this.partitionData.collection.name + ':   ... Organised ' + i + ' documents (' + percent + '%)');
				}

				if (fileStructure[record.account] == undefined)
					fileStructure[record.account] = {};
				// create the top level

				if (fileStructure[record.account][record.network] == undefined)
					fileStructure[record.account][record.network] = {};
				// does the network exist

				var day = moment.utc(record.timestamp).format('DD-MM-YYYY'),
					target = (record.status) ? 'status' : record.target;

				if (fileStructure[record.account][record.network][target] == undefined)
					fileStructure[record.account][record.network][target] = [];
				// target

				if (fileStructure[record.account][record.network][target][day] == undefined)
					fileStructure[record.account][record.network][target][day] = [];
				// dates

				fileStructure[record.account][record.network][target][day].push(record);
				// push the record

				i++;
				c++;
				// update counters
			});

			stream.on('error', function(err)
			{
				error(err);
			});

			stream.on('close', function()
			{
				callback(null, percent);
				// do the writing outside of here
			});
		},
		function (percent, callback)
		{
			if (percent != 100)
				notice(_this.partitionData.collection.name + ':   ... Organised ' + _this.pcount + ' documents (100%)');
			
			success('Successfully completed organising');
			// once we've got to here we're safe in saying our data has been analysed

			_this.e.emit('finished organising', fileStructure);
			// emit an event so we can continue in another file
		}
	]);
	// series our thingies so we know what we're doing.
};

/*
 * Database::finish
 *
 * Write the meta data and delete the old records
 */
Database.finish = function(callback)
{
	var _this = this;
		_this.dump = require('./dump').dump;

	notice('Attempting to write meta data to database...');
	_this.metaData.collection.insert(_this.dump.metaData, {safe: true}, function(err, records)
	{
		if (err)
		{
			error_noshut('Failed to write meta data to database...');
			_this.dump.dumpMetaData();
		}
		else
		{
			success('Successfully wrote meta data to database');
			// meta data dumped, lets delete the old records
		}

		_this.partitionData.count(_this.dump.query, function(err, pcount)
		{
			//_this.partitionData.remove(_this.dump.query, function(err)
			//{
				success('Successfully removed old documents from the database, ' + pcount + ' documents removed');
				callback();
			//});
			// delete the old records
		});
		// count the old records
	});
	// attempt to save data to database
};