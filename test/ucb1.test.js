/**
 * Module dependencies
 */
var should = require("should")
  , pivot = require("pivot")
  , express = require("express")
  , supertest = require("supertest")
  , Batch = require("batch")
  , UCB1 = require("..")
  , Bernoulli = require("./arms/bernoulli");

describe("ucb", function(){

  var experiments, app, arms;

  beforeEach(function() {
    experiments = pivot()
    experiments.use(new UCB1());

    app = express();

    app.use(express.cookieParser());
    app.use(experiments);
    app.use(app.router);

    app.get("/", function(req, res) {
      res.send(""+req.feature("test1"));
    });

    app.post("/", function(req, res) {
      var arm = arms[parseInt(req.feature("test1"))-1];
      req.reward(arm.draw());
      res.send(204);
    });
  });

  it("should converge to the correct variant", function(done) {
    // Setup the arms
    arms = [
      new Bernoulli(.15),
      new Bernoulli(.1),
      new Bernoulli(.1)
    ];

    experiments
      .feature("test1")
      .variants([1,2,3])
      .create(function(err) {
        if(err) return done(err);

        var batch = new Batch;

        batch.concurrency(1);

        for (var i = 0; i < 120; i++) {
          batch.push(function(cb) {
            supertest(app)
              .get("/")
              .on("error", cb)
              .end(function(err, res) {
                var arm = res.text
                  , cookie = "pivot="+encodeURIComponent(JSON.stringify({test1:arm}));

                supertest(app)
                  .post("/")
                  .set({cookie: cookie})
                  .on("error", cb)
                  .end(function(err, res) {
                    cb(null, arm);
                  });
              });
          });
        };

        batch.end(function(err, results) {
          if (err) return done(err);

          var counts = {1:0, 2:0, 3:0};
          results.forEach(function(result) {
            counts[result]++;
          });
          console.log(counts);
          done();
        });
      });
  });

});
