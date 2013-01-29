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
	  util = require('util'),
	  error = function(text) { util.log(clc.red(text)) },
	  success = function(text) { util.log(clc.green(text)) },
	  notice = function(text) { util.log(clc.yellow(text)) };

/*
 * Database
 *
 * A database object which contains all settings and handles connections and lookups
 */
var Database = {
	config: require('../config.json'),
	mongoose: require('mongoose')
};

/*
 * Database::setup
 *
 * Determine what backend we're using and how to deal with it
 */
Database.setup = function()
{
	var _this = this,
		ObjectId = _this.mongoose.Schema.ObjectId,
		databaseUrl = _this.config.url,
		database = _this.config.database,
		collection = _this.config.collection || 'buffers',
		metaCollection = _this.config.metaCollection || 'buffersMeta';

	_this.e = new EventEmitter();
	// create an event emitter

	if (databaseUrl == undefined)
		error('You have not provided a url or database to connect to');
	// no mongodb url, bail

	_this.conn = _this.mongoose.createConnection(databaseUrl + '/' + database, {db: {native_parser: true}});

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
						_this.conn.db.createCollection(metaCollection, {capped: false}, function(err, collection) {
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
				var MetaDataModel = new _this.mongoose.Schema({
					account: String,
					network: ObjectId,
					target: String,
					timestamp: Number,
					baseLocation: String,
					location: String
				});

				_this.metaData = _this.mongoose.model(metaCollection, MetaDataModel);
				// setup the schema

				notice('Metadata collection:    setting up schema');
				// setup our models and schemas here

				callback();
			},
			function(callback)
			{
				_this.e.emit('complete');
				// once all this is done emit the complete callback so we can continue

				callback();
			}
		]);
		// execute tasks in order
	});
	// connected, move on.
};

exports.database = Database;