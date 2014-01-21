
/**
 * Module dependencies
 */

var Promise = require('bluebird'),
    Db = require('mongodb').Db,
    Server = require('mongodb').Server;

/**
 * Manage a connection to a Mongo Server
 *
 * @param {Object} config
 * @return {Object}
 * @api private
 */

var Connection = module.exports = function Connection(config) {

  // Hold the config object
  this.config = config || {};

  // Build Database connection
  this.buildConnection();

  return this;
};


/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS
/////////////////////////////////////////////////////////////////////////////////


/**
 * Create A Collection
 *
 * @param {String} name
 * @param {Object} collection
 * @param {Function} callback
 * @api public
 */

Connection.prototype.createCollection = function createCollection(name, collection, cb) {
  var self = this;

  var promise = new Promise(function (resolve, reject) {
    // Open a Connection, handles errors by itself
    self.database.openAsync()
    .then(function() {
      // Create the Collection
      return self.database.createCollectionAsync(name)
      .then(function(result) {
        // Create Indexes
        return self.ensureIndexes(result, collection.indexes, function(err) {
          return self.close()
          .then(function() {
            reject(err);
          });
        });
      })
      .error(function (err) {
        return self.close()
        .then(function() {
          reject(err);
        });
      });
    });
  });

  if (cb) {
    promise.nodeify(cb);
  }

  return promise;
};

/**
 * Drop A Collection
 *
 * @param {String} name
 * @param {Function} callback
 * @api public
 */

Connection.prototype.dropCollection = function dropCollection(name, cb) {
  var self = this;

  var promise = new Promise(function (resolve, reject) {
    // Open a Connection, handles errors by itself
    self.database.openAsync()
    .then(function() {
      // Drop the collection
      return self.database.dropCollectionAsync(name)
      .then(function(err) {
        return self.close()
        .then(function() {
          reject(err);
        });
      });
    });
  });

  if (cb) {
    promise.nodeify(cb);
  }

  return promise;
};


/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////


/**
 * Build Server and Database Connection Objects
 *
 * @api private
 */

Connection.prototype.buildConnection = function buildConnection() {

  // Set Safe Mode
  var safe = this.config.safe ? 1 : 0;

  // Build up options used for creating a Server instance
  var serverOptions = {
    native_parser: this.config.nativeParser,
    auth: {
      user: this.config.user,
      password: this.config.password
    }
  };

  // Build up options used for creating a Database instance
  var databaseOptions = {
    w: safe,
    native_parser: this.config.nativeParser
  };

  this.server = new Server(this.config.host, this.config.port, serverOptions);
  this.database = new Db(this.config.database, this.server, databaseOptions);

  Promise.promisifyAll(this.server);
  Promise.promisifyAll(this.database);
};


/**
 * Open a Connection
 *
 * Open a new Mongo connection.
 *
 * @param {Function} callback
 * @api private
 */

Connection.prototype.open = function open(cb) {
  var self = this;

  var promise = new Promise(function (resolve, reject) {
    self.database.openAsync()
    .then(function () {
      return self.authenticate(cb);
    })
    .error(function(err) {
      reject(err);
    });
  });

  if (cb) {
    promise.nodeify(cb);
  }

  return promise;
};

/**
 * Close a Connection
 *
 * Closes an open Connection object
 *
 * @param {Function} callback
 * @api private
 */

Connection.prototype.close = function close(cb) {
  return this.database.closeAsync().nodeify(cb);
};

/**
 * Authenticate A Connection
 *
 * @param {Function} callback
 * @api private
 */

Connection.prototype.authenticate = function authenticate(cb) {
  var self = this,
      options = this.database.serverConfig.options;

  var promise = new Promise(function (resolve, reject) {
    if(!options.auth.user && !options.auth.password) return resolve();

    var user = options.auth.user || '';
    var password = options.auth.password || '';

    self.database.authenticateAsync(user, password)
    .then(function(success) {
      // The authentication was a success, the database should now be authenticated
      resolve();
    })
    .error(function (err) {
      // The authentication was unsuccessful
      self.close(function() {
        if(err) return reject(err);

        reject(new Error('Could not authenticate the User/Password combination provided.'));
      });
    });
  });

  if (cb) {
    promise.nodeify(cb);
  }

  return promise;
};

/**
 * Ensure Indexes
 *
 * @param {String} collection
 * @param {Array} indexes
 * @param {Function} callback
 * @api private
 */

Connection.prototype.ensureIndexes = function ensureIndexes(collection, indexes, cb) {
  function createIndex(item) {
    return collection.ensureIndex(item.index, item.options);
  }

  var promise = Promise.all(indexes).then(createIndex);

  if (cb) {
    promise.nodeify(cb);
  }

  return promise;
};
