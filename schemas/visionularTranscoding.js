const mongoose = require("mongoose");

// VisonularTranscoding schema - minimal fields for task lookup
const visionularTranscodingSchema = mongoose.Schema(
  {
    _id: mongoose.Schema.Types.ObjectId,
    task_id: {
      type: String,
      required: true,
      index: true, // Add index for faster lookups
    },
  },
  {
    timestamps: true,
    versionKey: false,
  }
);

module.exports = {
  visionularTranscodingSchema,
};
