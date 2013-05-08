/**
 * Module dependencies
 */
var redis = require("redis")
  , debug = require("debug")("pivot:assign:mab-ucb1")
  , url = require("url");

/**
 * Expose assign
 */
function UCB1Strategy(options, client) {
  options = options || {};
  client = client || "redis://localhost:6379";

  if(typeof client === "string") client = createClient(client);

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

  // Store the feature in our features list
  create.sadd(join(prefix,"features"), name);

  // TODO don't re-add untested arms
  create.sadd(join(prefix,"untested"), feature.variants.map(function(value, idx) {
    return idx;
  }));

  // Is it wip?
  create.hset(join(prefix,"config"), "wip", feature.wip ? 1 : 0);

  // Set to not release the variant by default
  create.hsetnx(join(prefix,"config"), "released", 0);

  // Setup any new variants
  feature.variants.forEach(function(variant) {
    create.hsetnx(join(prefix,"counts"), variant, 0);
    create.hsetnx(join(prefix,"values"), variant, "0.0");
  });

  // Store the arms for admin ui
  create.set(join(prefix,"arms"), JSON.stringify(feature.variants));

  // Exectute the setup
  create.exec(function(err) {
    if(err) return cb(err);

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
  if (Object.keys(features).length) return cb(null, features);

  // Use it for later
  features = {};

  // TODO have a callback that asks the app how many/which features to add the user for context testing
  // For now, we'll just enable the first feature
  var name = Object.keys(self.features)[0];

  var assign = client.multi()
    , prefix = join(prefix,"features",name);

  assign.hmgetall(join(prefix,"config")); // 0
  assign.spop(join(prefix,"untested")); // 1
  assign.hmgetall(join(prefix,"counts")); // 2
  assign.hmgetall(join(prefix,"values")); // 3

  assign.exec(function(err, feature) {
    if (err) return cb(err);

    // It hasn't been released
    if (feature[0].wip || !feature[0].released) return cb(null, {});

    // We've got an untested arm
    var idx;
    if ((idx = feature[1])) {
      features[name] = self.features[name].variants[idx];
      return cb(null, features);
    }

    // Let's do some testing
    var counts = feature[2]
      , values = feature[3]
      , total = sum(counts);

    var ubcValues = self.features[name].variants.map(function(variant) {
      return values[variant] + Math.sqrt((2 * Math.log(total)) / parseFloat(counts[variant]));
    });

    var selectedArm = Math.max.apply(null, ubcValues);

    features[name] = self.features[name].variants[selectedArm];

    cb(null, features);
  });
};

UCB1Strategy.prototype.install = function(req, res, user, features) {
  var self = this
    , prefix = join(this.prefix,"features");

  req.reward = function(reward) {

    var update = self.client.multi();

    Object.keys(features).forEach(function(name) {
      update.hincrby(join(prefix,name,counts,features[name]),1); // Increment the count for the arm
      // TOOD get the new count for the chosen arm
      // TODO get the value for the arm
      // TODO set the new value for the arm
      // new_value = ((n - 1) / float(n)) * value + (1 / float(n)) * reward
    });
  };
};

function join() {
  return Array.prototype.join.call(arguments, ":");
};

function sum(obj) {
  return Object.keys(obj).reduce(function(prev, current, idx) {
    return prev + current;
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
