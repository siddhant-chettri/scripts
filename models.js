const mongoose = require("mongoose");
const { RawMediaSchema } = require("./schemas/rawMedia");
const { episodeSchema } = require("./schemas/episode");

// Create models
const RawMedia = mongoose.model("RawMedia", RawMediaSchema);
const Episode = mongoose.model("Episode", episodeSchema);

module.exports = {
  RawMedia,
  Episode,
};
