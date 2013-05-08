/**
 * Module dependencies
 */
var redis = require("redis")
  , debug = require("debug")("pivot:assign:mab-ucb1")
  , url = require("url");

/**
 * Expose the strategy
 */
module.exports = UCB1Strategy;

function UCB1Strategy(options, client) {
  options = options || {};
  client = client || "redis://localhost:6379";

  if(typeof client === "string") client = createClient(client);

  this.name = "ucb1";
  this.client = client;
  this.appName = options.appName || "app";
  this.prefix = options.prefix || join("pivot","ucb1",this.appName);
  this.features = {};
};

UCB1Strategy.prototype.create = function(feature, cb) {
  var create = this.client.multi()
    , name = feature.name
    , prefix = join(this.prefix,"features",name)
    , self = this
    , untested = [];

  debug("creating feature", prefix);

  // Store the feature in our features list
  create.sadd(join(this.prefix,"features"), name);

  // TODO don't re-add untested arms
  create.sadd(join(prefix,"untested"), feature.variants.map(function(value, idx) {
    return idx;
  }));

  create.hset(join(prefix,"config"), "name", name);

  // Is it wip?
  feature.wip ?
    create.hset(join(prefix,"config"), "wip", 1) :
    create.hdel(join(prefix,"config"), "wip");

  // Setup any new variants
  feature.variants.forEach(function(variant) {
    create.hsetnx(join(prefix,"counts"), variant, 0);
    create.hsetnx(join(prefix,"values"), variant, "0.0");
  });

  // Store the arms for admin ui
  create.set(join(prefix,"arms"), JSON.stringify(feature.variants));

  // Exectute the setup
  create.exec(function(err, results) {
    if(err) return cb(err);

    debug(results);

    // Store it locally for fast access
    self.features[name] = feature;

    cb();
  });
};

UCB1Strategy.prototype.assign = function(user, features, cb) {
  var client = this.client
    , prefix = this.prefix
    , self = this;

  // If the user is already apart of an experiment, don't assign them
  debug("current features", features);
  if (Object.keys(features).length) return cb(null, features);

  // Use it for later
  features = {};

  // TODO have a callback that asks the app how many/which features to add the user for context testing
  // For now, we'll just enable the first feature
  var name = Object.keys(self.features)[0];

  if(!name) return cb(null, features);

  var assign = client.multi()
    , prefix = join(prefix,"features",name);

  assign.hgetall(join(prefix,"config")); // 0
  assign.spop(join(prefix,"untested")); // 1
  assign.hgetall(join(prefix,"counts")); // 2
  assign.hgetall(join(prefix,"values")); // 3

  assign.exec(function(err, feature) {
    if (err) return cb(err);

    // It hasn't been released
    debug("config",feature[0]);
    if (feature[0].wip || !feature[0].released) {
      debug(name+" has not been released");
      return cb(null, {});
    }

    // We've got an untested arm
    var idx;
    if ((idx = feature[1])) {
      features[name] = self.features[name].variants[idx];
      debug("untested arm", name, features[name]);
      return cb(null, features);
    }

    // Let's do some testing
    var counts = feature[2]
      , values = feature[3]
      , total = sum(counts);

    debug("counts", counts);
    debug("values", values);
    debug("total", total);

    var ubcValues = self.features[name].variants.map(function(variant) {
      debug(values[variant]+" + Math.sqrt( 2 * Math.log( "+total+" ) / "+counts[variant]+" ))");
      return parseFloat(values[variant]) + Math.sqrt(2 * Math.log(total) / parseFloat(counts[variant]));
    });

    debug("ubc values", ubcValues);

    var selectedArm = Math.max.apply(null, ubcValues)
      , selectedIndex = ubcValues.indexOf(selectedArm);

    // We haven't got any data yet so a random guess is as good as any
    if(!isFinite(selectedArm)) selectedArm = Math.floor(Math.random() * self.features[name].variants.length);

    debug("selected arm", selectedArm, selectedIndex);

    features[name] = self.features[name].variants[selectedIndex];

    cb(null, features);
  });
};

UCB1Strategy.prototype.install = function(req, res, user, features) {
  var self = this
    , prefix = join(this.prefix,"features");

  req.reward = function(reward) {

    var keys = Object.keys(features)
      , incrCount = self.client.multi();

    debug("rewarding arms with "+reward, keys);

    keys.forEach(function(name) {
      incrCount.hincrby(join(prefix,name,"counts"), features[name], 1); // Increment the count for the arm
      incrCount.hget(join(prefix,name,"values"), features[name]);
    });

    incrCount.exec(function(err, results) {
      var update = self.client.multi();

      debug("incr results", results);

      keys.forEach(function(name, idx) {
        var count = parseInt(results[2*idx])
          , value = parseFloat(results[2*idx+1]);

        debug("current settings", name, count, value);

        debug("old value", value);

        value = ((count - 1) / count) * value + (1 / count) * reward;
        debug("new value", value);

        update.hset(join(prefix,name,"values"), features[name], value);
      });

      update.exec(function() {
        if (err) console.error(err);
      });
    });
  };
};

function join() {
  return Array.prototype.join.call(arguments, ":");
};

function sum(obj) {
  return Object.keys(obj).reduce(function(prev, current, idx) {
    return parseInt(prev) + parseInt(current);
  });
};

/**
 * Create a redis client from a url
 *
 * @api private
 */
function createClient(redisUrl) {
  var options = url.parse(redisUrl)
    , client = redis.createClient(options.port, options.hostname);

  // Authorize the connection
  if (options.auth) client.auth(options.auth.split(":")[1]);

  // Exit gracefully
  function close() {
    client.end();
  };
  process.once("SIGTERM", close);
  process.once("SIGINT", close);

  return client
};
