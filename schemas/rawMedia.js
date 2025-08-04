const mongoose = require("mongoose");
const {
  SourceTypeEnum,
  MediaStatusEnum,
  TranscodingEngineEnum,
  TranscodingTaskTypeEnum,
  TaskStatusEnum,
} = require("../enums");

// Embedded schemas
const SourceSchema = new mongoose.Schema(
  {
    type: {
      type: String,
      enum: Object.values(SourceTypeEnum),
      required: true,
    },
    url: {
      type: String,
      required: false,
    },
  },
  { _id: false }
);

const DestinationSchema = new mongoose.Schema(
  {
    url: {
      type: String,
      required: true,
    },
  },
  { _id: false }
);

const MediaStatusHistorySchema = new mongoose.Schema(
  {
    status: {
      type: String,
      enum: Object.values(MediaStatusEnum),
      required: true,
    },
    timestamp: {
      type: Date,
      required: true,
    },
  },
  { _id: false }
);

const TranscodingTaskSchema = new mongoose.Schema(
  {
    completedAt: {
      type: Date,
      required: false,
    },
    config: {
      type: mongoose.Schema.Types.Mixed,
      required: false,
    },
    createdAt: {
      type: Date,
      required: false,
      default: Date.now,
    },
    externalTaskId: {
      type: String,
      required: false,
    },
    result: {
      type: mongoose.Schema.Types.Mixed,
      required: false,
    },
    taskStatus: {
      type: String,
      enum: Object.values(TaskStatusEnum),
      required: true,
    },
    taskType: {
      type: String,
      enum: Object.values(TranscodingTaskTypeEnum),
      required: true,
    },
    transcodingEngine: {
      type: String,
      enum: Object.values(TranscodingEngineEnum),
      required: true,
    },
    transcodingTaskId: {
      type: mongoose.Schema.Types.ObjectId,
      required: false,
    },
  },
  { _id: false }
);

// Main RawMedia schema
const RawMediaSchema = new mongoose.Schema(
  {
    contentType: {
      type: String,
      required: true,
    },
    destination_url: String,
    durationInSeconds: {
      type: Number,
      required: false,
    },
    source_type: String,
    destination_url: String,
    status: {
      type: String,
      enum: Object.values(MediaStatusEnum),
      required: true,
    },
    statusHistory: {
      type: [MediaStatusHistorySchema],
      default: [],
    },
    transcodingTask: {
      type: [
        {
          taskStatus: String,
          taskType: String,
          transcodingEngine: String,
          transcodingTaskId: mongoose.Schema.Types.ObjectId,
        },
      ],
      default: [],
    },
    uploadProgress: {
      type: Number,
      default: 0,
    },
  },
  {
    timestamps: true,
  }
);

module.exports = {
  SourceSchema,
  DestinationSchema,
  MediaStatusHistorySchema,
  TranscodingTaskSchema,
  RawMediaSchema,
};
