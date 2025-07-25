const mongoose = require("mongoose");
const { exec } = require("child_process");
const { promisify } = require("util");

const execAsync = promisify(exec);

require("dotenv").config();

const MONGO_URI = process.env.MONGO_URI;
const MEDIA_VIDEO_URL = process.env.MEDIA_VIDEO_URL;

// Import schemas, models, and enums from the new organized structure
const { RawMedia, Episode } = require("./models");

console.log("🚀 Script started at:", new Date().toISOString());
console.log("📋 Configuration:");
console.log("  - MongoDB URI:", MONGO_URI ? "✅ Set" : "❌ Missing");
console.log("  - Media Video URL:", MEDIA_VIDEO_URL ? "✅ Set" : "❌ Missing");

// Function to get video duration using ffprobe
async function getVideoDurationWithFFmpeg(videoUrl) {
  console.log(`🎬 Getting video duration for: ${videoUrl}`);

  try {
    const command = `ffprobe -v quiet -show_entries format=duration -of csv=p=0 "${videoUrl}"`;
    console.log(`🔧 Executing command: ${command}`);

    const startTime = Date.now();
    const { stdout, stderr } = await execAsync(command);
    const executionTime = Date.now() - startTime;

    console.log(`⏱️  FFprobe execution time: ${executionTime}ms`);
    console.log(`📤 FFprobe stdout: "${stdout.trim()}"`);

    if (stderr) {
      console.warn(`⚠️  FFprobe warning for ${videoUrl}:`, stderr);
    }

    const duration = parseFloat(stdout.trim());

    if (isNaN(duration)) {
      console.error(
        `❌ Could not parse duration from ffprobe output: "${stdout}"`
      );
      throw new Error(
        `Could not parse duration from ffprobe output: ${stdout}`
      );
    }

    const roundedDuration = Math.round(duration);
    console.log(
      `✅ Video duration determined: ${roundedDuration} seconds (${Math.floor(
        roundedDuration / 60
      )}:${String(roundedDuration % 60).padStart(2, "0")})`
    );

    return roundedDuration; // Return duration in seconds, rounded to nearest integer
  } catch (error) {
    console.error(`💥 Error getting duration for ${videoUrl}:`, error.message);
    throw new Error(`Failed to get video duration: ${error.message}`);
  }
}

// Connect to MongoDB
async function connectDB() {
  console.log("🔌 Attempting to connect to MongoDB...");
  console.log("📊 Connection parameters:");
  console.log("  - Server Selection Timeout: 10000ms");
  console.log("  - Connect Timeout: 10000ms");
  console.log("  - Socket Timeout: 10000ms");
  console.log("  - Max Pool Size: 10");

  try {
    const startTime = Date.now();
    await mongoose.connect(MONGO_URI, {
      serverSelectionTimeoutMS: 10000, // 30 seconds
      connectTimeoutMS: 10000, // 30 seconds
      socketTimeoutMS: 10000, // 30 seconds
      maxPoolSize: 10,
      retryWrites: true,
      w: "majority",
    });
    const connectionTime = Date.now() - startTime;
    console.log(`✅ Connected to MongoDB successfully in ${connectionTime}ms`);
    console.log(`📍 Database: ${mongoose.connection.db.databaseName}`);
    console.log(
      `🏠 Host: ${mongoose.connection.host}:${mongoose.connection.port}`
    );
  } catch (error) {
    console.error("💥 MongoDB connection error:", error);
    console.error("🔍 Error details:", {
      name: error.name,
      message: error.message,
      code: error.code,
    });
    process.exit(1);
  }
}

async function findActiveEpisodesWithMissingRawMediaId(
  transcodingType = "visionularHls"
) {
  console.log(
    `🔍 Searching for active episodes with missing rawMediaId in ${transcodingType}...`
  );

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

    console.log(`📋 Query being executed:`, JSON.stringify(query, null, 2));

    const startTime = Date.now();
    const episodes = await Episode.find(query).lean();
    const queryTime = Date.now() - startTime;

    console.log(`⏱️  Query execution time: ${queryTime}ms`);
    console.log(`📊 Episodes found: ${episodes.length}`);

    if (episodes.length === 0) {
      console.log(
        `ℹ️  No active episodes found with missing rawMediaId in ${transcodingType}.`
      );
    } else {
      console.log(`📋 Episode details summary:`);
      episodes.forEach((episode, index) => {
        console.log(
          `  ${index + 1}. Slug: ${episode.slug}, Type: ${
            episode.type
          }, Source: ${episode.sourceLink}`
        );
      });
    }

    return episodes;
  } catch (error) {
    console.error("💥 Error finding episodes:", error);
    console.error("🔍 Error details:", {
      name: error.name,
      message: error.message,
      stack: error.stack,
    });
    throw error;
  }
}

async function createRawMediaFromEpisode(episode) {
  console.log(
    `🏗️  Creating raw media for episode: ${episode.slug} (${episode.type})`
  );
  console.log(`📂 Source link: ${episode.sourceLink}`);

  // Get actual video duration using ffmpeg
  const videoUrl = `${MEDIA_VIDEO_URL}/${episode.sourceLink}`;
  console.log(`🔗 Full video URL: ${videoUrl}`);

  let durationInSeconds;
  durationInSeconds = await getVideoDurationWithFFmpeg(videoUrl);
  console.log(`✅ Duration obtained: ${durationInSeconds} seconds`);

  // Validate episode data before creating raw media
  console.log(`🔍 Validating episode data...`);
  if (!episode.visionularHls?.visionularTaskId) {
    console.warn(
      `⚠️  Missing visionularHls.visionularTaskId for episode ${episode.slug}`
    );
  }
  if (!episode.visionularHls265?.visionularTaskId) {
    console.warn(
      `⚠️  Missing visionularHls265.visionularTaskId for episode ${episode.slug}`
    );
  }

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

  console.log(
    `📋 Raw media data to be created:`,
    JSON.stringify(rawMediaData, null, 2)
  );

  try {
    const startTime = Date.now();
    const rawMedia = await RawMedia.create(rawMediaData);
    const creationTime = Date.now() - startTime;

    console.log(`✅ Raw media created successfully in ${creationTime}ms`);
    console.log(`🆔 Raw media ID: ${rawMedia._id}`);
    console.log(
      `📊 Raw media document size: ${
        JSON.stringify(rawMedia).length
      } characters`
    );

    return rawMedia;
  } catch (error) {
    console.error(
      `💥 Error creating raw media for episode ${episode.slug}:`,
      error
    );
    console.error("🔍 Error details:", {
      name: error.name,
      message: error.message,
      validationErrors: error.errors,
    });
    throw error;
  }
}

// Placeholder function - implement based on your business logic
async function addRawMediaToEnAndHinEpisode({ rawMediaId, slug, type }) {
  console.log(`🔗 Adding raw media ID to episode: ${slug} (${type})`);
  console.log(`🆔 Raw media ID: ${rawMediaId}`);

  try {
    const updateData = {
      $set: {
        "visionularHls.rawMediaId": rawMediaId,
        "visionularHls265.rawMediaId": rawMediaId,
      },
    };

    console.log(`📋 Update data:`, JSON.stringify(updateData, null, 2));

    const startTime = Date.now();
    const result = await Episode.updateOne({ slug, type }, updateData);
    const updateTime = Date.now() - startTime;

    console.log(`⏱️  Update execution time: ${updateTime}ms`);
    console.log(`📊 Update result:`, {
      acknowledged: result.acknowledged,
      matchedCount: result.matchedCount,
      modifiedCount: result.modifiedCount,
    });

    if (result.matchedCount === 0) {
      console.warn(`⚠️  No episode found with slug: ${slug} and type: ${type}`);
    } else if (result.modifiedCount === 0) {
      console.warn(
        `⚠️  Episode found but no modifications made for slug: ${slug}`
      );
    } else {
      console.log(`✅ Episode updated successfully: ${slug}`);
    }
  } catch (error) {
    console.error(`💥 Error updating episode ${slug}:`, error);
    console.error("🔍 Error details:", {
      name: error.name,
      message: error.message,
    });
    throw error;
  }
}

// Main execution function
async function main() {
  const scriptStartTime = Date.now();
  console.log("🎯 Starting main execution...");

  try {
    // Connect to database first
    await connectDB();

    console.log("🔄 Starting processing...");

    // Phase 1: Process visionularHls
    console.log("\n📺 Phase 1: Processing visionularHls episodes");
    console.log("=".repeat(50));

    const episodesWithMissingRawMediaIdInVisionularHls =
      await findActiveEpisodesWithMissingRawMediaId("visionularHls");

    if (episodesWithMissingRawMediaIdInVisionularHls.length === 0) {
      console.log(
        "ℹ️  No episodes with missing raw media id in visionular hls to process."
      );
    } else {
      console.log(
        `🎬 Found ${episodesWithMissingRawMediaIdInVisionularHls.length} episodes to process.`
      );

      for (
        let i = 0;
        i < episodesWithMissingRawMediaIdInVisionularHls.length;
        i++
      ) {
        const episode = episodesWithMissingRawMediaIdInVisionularHls[i];
        const episodeStartTime = Date.now();

        console.log(
          `\n📍 Processing episode ${i + 1}/${
            episodesWithMissingRawMediaIdInVisionularHls.length
          }`
        );
        console.log(`   Slug: ${episode.slug}`);
        console.log(`   Type: ${episode.type}`);

        try {
          const rawMedia = await createRawMediaFromEpisode(episode);
          await addRawMediaToEnAndHinEpisode({
            rawMediaId: rawMedia._id,
            slug: episode.slug,
            type: episode.type,
          });

          const episodeTime = Date.now() - episodeStartTime;
          console.log(`✅ Episode ${i + 1} completed in ${episodeTime}ms`);
        } catch (error) {
          console.error(
            `💥 Failed to process episode ${i + 1} (${episode.slug}):`,
            error.message
          );
          // Continue with next episode instead of failing completely
        }
      }
    }

    // Phase 2: Process visionularHls265
    console.log("\n📺 Phase 2: Processing visionularHls265 episodes");
    console.log("=".repeat(50));

    const episodesWithMissingRawMediaIdInVisionularHls265 =
      await findActiveEpisodesWithMissingRawMediaId("visionularHls265");

    if (episodesWithMissingRawMediaIdInVisionularHls265.length === 0) {
      console.log(
        "ℹ️  No episodes with missing raw media id in visionular hls 265 to process."
      );
    } else {
      console.log(
        `🎬 Found ${episodesWithMissingRawMediaIdInVisionularHls265.length} episodes to process.`
      );

      for (
        let i = 0;
        i < episodesWithMissingRawMediaIdInVisionularHls265.length;
        i++
      ) {
        const episode = episodesWithMissingRawMediaIdInVisionularHls265[i];
        const episodeStartTime = Date.now();

        console.log(
          `\n📍 Processing HLS265 episode ${i + 1}/${
            episodesWithMissingRawMediaIdInVisionularHls265.length
          }`
        );
        console.log(`   Slug: ${episode.slug}`);
        console.log(`   Type: ${episode.type}`);

        try {
          if (!episode.visionularHls?.rawMediaId) {
            console.warn(
              `⚠️  Episode ${episode.slug} missing visionularHls.rawMediaId, skipping...`
            );
            continue;
          }

          console.log(
            `🔗 Copying rawMediaId from visionularHls: ${episode.visionularHls.rawMediaId}`
          );

          episode.visionularHls265.rawMediaId =
            episode.visionularHls.rawMediaId;

          const startTime = Date.now();
          await episode.save();
          const saveTime = Date.now() - startTime;

          const episodeTime = Date.now() - episodeStartTime;
          console.log(
            `✅ Episode ${
              i + 1
            } updated in ${saveTime}ms (total: ${episodeTime}ms)`
          );
        } catch (error) {
          console.error(
            `💥 Failed to update HLS265 episode ${i + 1} (${episode.slug}):`,
            error.message
          );
          // Continue with next episode instead of failing completely
        }
      }
    }

    const totalTime = Date.now() - scriptStartTime;
    console.log("\n🎉 Processing completed successfully!");
    console.log(
      `⏱️  Total execution time: ${totalTime}ms (${Math.round(
        totalTime / 1000
      )}s)`
    );
  } catch (error) {
    console.error("💥 Main execution error:", error);
    console.error("🔍 Error details:", {
      name: error.name,
      message: error.message,
      stack: error.stack,
    });
  } finally {
    console.log("\n🔌 Closing database connection...");
    const closeStartTime = Date.now();
    await mongoose.connection.close();
    const closeTime = Date.now() - closeStartTime;
    console.log(`✅ Database connection closed in ${closeTime}ms`);

    const totalTime = Date.now() - scriptStartTime;
    console.log(`🏁 Script finished at: ${new Date().toISOString()}`);
    console.log(
      `⏱️  Total script runtime: ${totalTime}ms (${Math.round(
        totalTime / 1000
      )}s)`
    );
  }
}

// Run the script
main();
