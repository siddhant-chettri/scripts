const mongoose = require("mongoose");
const { RawMediaSchema } = require("./schemas/rawMedia");
const { episodeSchema } = require("./schemas/episode");

// Create models
const RawMedia = mongoose.model("raw-media", RawMediaSchema, "raw-media");
const Episode = mongoose.model("episodes", episodeSchema, "episodes");

module.exports = {
  RawMedia,
  Episode,
};
