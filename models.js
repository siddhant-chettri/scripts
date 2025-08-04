const mongoose = require("mongoose");
const { RawMediaSchema } = require("./schemas/rawMedia");
const { episodeSchema } = require("./schemas/episode");
const {
  visionularTranscodingSchema,
} = require("./schemas/visionularTranscoding");

// Create models
const RawMedia = mongoose.model("raw-media", RawMediaSchema, "raw-media");
const Episode = mongoose.model("episodes", episodeSchema, "episodes");
const VisonularTranscoding = mongoose.model(
  "VisionularTranscoding",
  visionularTranscodingSchema,
  "VisionularTranscoding"
);

module.exports = {
  RawMedia,
  Episode,
  VisonularTranscoding,
};
