const mongoose = require("mongoose");
const { exec } = require("child_process");
const { promisify } = require("util");

const execAsync = promisify(exec);

// Enums
const MediaTypeEnum = {
  ARTIST: "artist",
  COLLECTION_PERIPHERAL: "collection-peripheral",
  EPISODE: "episode",
  EPISODE_PERIPHERAL: "episode-peripheral",
  REEL: "reel",
  SHOW_EPISODE: "show-episode",
  SHOW_PERIPHERAL: "show-peripheral",
};

const SourceTypeEnum = {
  GOOGLE_DRIVE: "google-drive",
  LOCAL_UPLOAD: "local-upload",
};

const MediaStatusEnum = {
  CREATED: "created",
  TRANSCODING_COMPLETED: "transcoding-completed",
  TRANSCODING_FAILED: "transcoding-failed",
  TRANSCODING_STARTED: "transcoding-started",
  UPLOAD_COMPLETED: "upload-completed",
  UPLOAD_FAILED: "upload-failed",
  UPLOADING: "uploading",
};

const TranscodingEngineEnum = {
  AWS_MEDIA_CONVERT: "aws-media-convert",
  VISIONULAR: "visionular",
};

const TranscodingTaskTypeEnum = {
  VIDEO_TRANSCODING: "video-transcoding",
};

const TaskStatusEnum = {
  COMPLETED: "completed",
  FAILED: "failed",
  IN_PROGRESS: "in-progress",
};

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
    destination: {
      type: DestinationSchema,
      required: true,
    },
    durationInSeconds: {
      type: Number,
      required: false,
    },
    source: {
      type: SourceSchema,
      required: true,
    },
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
      type: [TranscodingTaskSchema],
      default: [],
    },
    uploadProgress: {
      type: Number,
      default: 0,
    },
  },
  {
    timestamps: true, // Adds createdAt and updatedAt
    collection: "rawmedia",
  }
);

// Episode schema
const episodeSchema = mongoose.Schema(
  {
    _id: Number,
    title: String,
    thumbnail: Object,
    type: String,
    startDate: Date,
    endDate: Date,
    metaTags: Array,
    sourceLink: String,
    hlsSourceLink: String,
    viewCount: Number,
    status: String,
    artistList: [
      {
        id: Number,
        name: String,
        slug: String,
        profilePic: String,
        gradient: String,
        status: String,
        city: String,
      },
    ],
    seasonId: Number,
    showId: Number,
    description: String,
    slug: String,
    language: String,
    genre: String,
    duration: Number,
    like: Number,
    location: String,
    tg: String,
    mood: String,
    theme: String,
    isExclusive: Number,
    collectionId: Number,
    order: Number,
    subGenreList: Array,
    categoryList: Array,
    parentDetail: Object,
    label: String,
    displayLanguage: String,
    isExclusiveOrder: Number,
    randomOrder: Number,
    feedRandomOrder: Number,
    genreList: [
      {
        id: Number,
        name: String,
      },
    ],
    skipDisclaimer: Boolean,
    showSlug: String,
    collectionSlug: String,
    mogiHLS: Object,
    videoClip: String,
    audioClip: String,
    parentDetail: Object,
    episodeOrder: Number,
    clips: Object,
    createdAt: Date,
    updatedAt: Date,
    isNewContent: Boolean,
    isPopularContent: Boolean,
    consumptionRateCount: Number,
    likeConsumptionRatio: Number,
    likeCount: Number,
    selectedPeripheral: Object,
    keywordSearch: String,
    freeEpisode: Boolean,
    freeEpisodeDuration: Number,
    isPremium: Boolean,
    mediaAccessTier: Number,
    premiumNessOrder: Number,
    deepLink: String,
    complianceRating: String,
    preContentWarningText: String,
    complianceList: Array,
    contentWarnings: Array,
    nextEpisodeNudgeStartTime: Number,
    internalSearchTags: String,
    themes: [
      {
        id: { type: mongoose.Schema.Types.Number, ref: "theme" },
        name: { type: String },
        hindiName: { type: String },
      },
    ],
    moods: [
      {
        id: { type: mongoose.Schema.Types.Number, ref: "mood" },
        name: { type: String },
        hindiName: { type: String },
      },
    ],
    descriptorTags: [
      {
        id: { type: mongoose.Schema.Types.Number, ref: "descriptorTag" },
        name: { type: String },
        hindiName: { type: String },
      },
    ],
    visionularHls: {
      type: Object,
      default: {},
    },
    visionularHls265: {
      type: Object,
      default: {},
    },
  },
  {
    timestamps: true,
    versionKey: false,
  }
);

// Create models
const RawMedia = mongoose.model("RawMedia", RawMediaSchema);
const Episode = mongoose.model("Episode", episodeSchema);

// Function to get video duration using ffprobe
async function getVideoDurationWithFFmpeg(videoUrl) {
  try {
    const command = `ffprobe -v quiet -show_entries format=duration -of csv=p=0 "${videoUrl}"`;
    const { stdout, stderr } = await execAsync(command);

    if (stderr) {
      console.warn(`FFprobe warning for ${videoUrl}:`, stderr);
    }

    const duration = parseFloat(stdout.trim());

    if (isNaN(duration)) {
      throw new Error(
        `Could not parse duration from ffprobe output: ${stdout}`
      );
    }

    return Math.round(duration); // Return duration in seconds, rounded to nearest integer
  } catch (error) {
    console.error(`Error getting duration for ${videoUrl}:`, error.message);
    throw new Error(`Failed to get video duration: ${error.message}`);
  }
}

// Connect to MongoDB
async function connectDB() {
  try {
    await mongoose.connect(MONGO_URI, {
      serverSelectionTimeoutMS: 10000, // 30 seconds
      connectTimeoutMS: 10000, // 30 seconds
      socketTimeoutMS: 10000, // 30 seconds
      maxPoolSize: 10,
      retryWrites: true,
      w: "majority",
    });
    console.log("Connected to MongoDB");
  } catch (error) {
    console.error("Connection error:", error);
    process.exit(1);
  }
}

async function findActiveEpisodesWithMissingRawMediaId(
  transcodingType = "visionularHls"
) {
  try {
    const query = {
      status: "active",
      $or: [
        { [`${transcodingType}.rawMediaId`]: { $exists: false } },
        { [`${transcodingType}.rawMediaId`]: "" },
        { [`${transcodingType}.rawMediaId`]: null },
      ],
      [transcodingType]: { $exists: true }, // 3650 3651 these ids do not  have visonular objects
    };
    const episodes = await Episode.find(query).lean();

    if (episodes.length === 0) {
      console.log("No active episodes found with missing rawMediaId.");
    }

    return episodes;
  } catch (error) {
    console.error("Error finding episodes:", error);
    throw error;
  }
}

async function createRawMediaFromEpisode(episode) {
  // Get actual video duration using ffmpeg
  const videoUrl = `${MEDIA_VIDEO_URL}/${episode.sourceLink}`;
  let durationInSeconds;
  durationInSeconds = await getVideoDurationWithFFmpeg(videoUrl);
  console.log(
    `Got duration for ${episode.sourceLink}: ${durationInSeconds} seconds`
  );

  // Create raw media document using the schema
  const rawMediaData = {
    contentType: "video/mp4",
    destination: {
      url: videoUrl,
    },
    durationInSeconds: durationInSeconds,
    source: {
      type: "local-upload", // Using enum value from SourceTypeEnum
      url: videoUrl,
    },
    status: "transcoding-completed", // Using enum value from MediaStatusEnum
    statusHistory: [],
    transcodingTask: [
      {
        taskStatus: "completed", // Using enum value from TaskStatusEnum
        taskType: "video-transcoding", // Using enum value from TranscodingTaskTypeEnum
        transcodingEngine: "visionular", // Using enum value from TranscodingEngineEnum
        transcodingTaskId: episode.visionularHls.visionularTaskId,
      },
      {
        taskStatus: "completed", // Using enum value from TaskStatusEnum
        taskType: "video-transcoding", // Using enum value from TranscodingTaskTypeEnum
        transcodingEngine: "visionular", // Using enum value from TranscodingEngineEnum
        transcodingTaskId: episode.visionularHls265.visionularTaskId,
      },
    ],
    uploadProgress: 100,
  };

  const rawMedia = await RawMedia.create(rawMediaData);
  return rawMedia;
}

// Placeholder function - implement based on your business logic
async function addRawMediaToEnAndHinEpisode({ rawMediaId, slug, type }) {
  await Episode.updateOne(
    { slug, type },
    {
      $set: {
        "visionularHls.rawMediaId": rawMediaId,
        "visionularHls265.rawMediaId": rawMediaId,
      },
    }
  );
}

// Main execution function
async function main() {
  try {
    // Connect to database first
    await connectDB();

    console.log("Starting processing...");

    const episodesWithMissingRawMediaIdInVisionularHls =
      await findActiveEpisodesWithMissingRawMediaId("visionularHls");

    if (episodesWithMissingRawMediaIdInVisionularHls.length === 0) {
      console.log(
        "No episodes with missing raw media id in visionular hls to process."
      );
      return;
    }

    console.log(
      `Found ${episodesWithMissingRawMediaIdInVisionularHls.length} episodes to process.`
    );

    // console.log(`Processing ${englishEpisodes.length} English episodes.`);

    for (const episode of episodesWithMissingRawMediaIdInVisionularHls) {
      const rawMedia = await createRawMediaFromEpisode(episode);
      await addRawMediaToEnAndHinEpisode({
        rawMediaId: rawMedia._id,
        slug: episode.slug,
        type: episode.type,
      });
    }

    const episodesWithMissingRawMediaIdInVisionularHls265 =
      await findActiveEpisodesWithMissingRawMediaId("visionularHls265");

    if (episodesWithMissingRawMediaIdInVisionularHls265.length === 0) {
      console.log(
        "No episodes with missing raw media id in visionular hls 265 to process."
      );
      return;
    }

    console.log(
      `Found ${episodesWithMissingRawMediaIdInVisionularHls265.length} episodes to process.`
    );

    for (const episode of episodesWithMissingRawMediaIdInVisionularHls265) {
      episode.visionularHls265.rawMediaId = episode.visionularHls.rawMediaId;
      await episode.save();
    }

    // console.log("Processing completed successfully.");
  } catch (error) {
    console.error("Main execution error:", error);
  } finally {
    await mongoose.connection.close();
    console.log("\nDatabase connection closed");
  }
}

// Run the script
main();
