function BernoulliArm(p) {
  this.p = p;
};

BernoulliArm.prototype.draw = function() {
  return Math.random() > this.p ? 0.0 : 1.0;
};

module.exports = BernoulliArm;
