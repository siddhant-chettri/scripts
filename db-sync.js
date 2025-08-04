const { exec } = require("child_process");
const { promisify } = require("util");
const fs = require("fs").promises;
const path = require("path");

const execAsync = promisify(exec);

require("dotenv").config();

// Environment variables
const REMOTE_URI = process.env.REMOTE_URI;
const LOCAL_URI = process.env.LOCAL_URI;
const SKIP_VERIFICATION = process.env.SKIP_VERIFICATION === "true";
const USE_PARALLEL = process.env.USE_PARALLEL !== "false"; // Default to true
const MAX_PARALLEL = parseInt(process.env.MAX_PARALLEL) || 3; // Max 3 collections in parallel
const USE_DIRECT_TRANSFER = process.env.USE_DIRECT_TRANSFER === "true"; // Direct DB-to-DB transfer
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 1000; // Documents per batch

// Specific collections to sync
const COLLECTIONS_TO_SYNC = [
  "contents",
  "shows",
  "episodes",
  "raw-media",
  "VisionularTranscoding",
  "adminusers",
  "cmsusers",
];

console.log("🔄 Database Sync Script started at:", new Date().toISOString());
console.log("📋 Configuration:");
console.log("  - Remote URI:", REMOTE_URI ? "✅ Set" : "❌ Missing");
console.log("  - Local URI:", LOCAL_URI ? "✅ Set" : "❌ Missing");
console.log("  - Skip Verification:", SKIP_VERIFICATION ? "✅ Yes" : "❌ No");
console.log(
  "  - Parallel Processing:",
  USE_PARALLEL ? "✅ Enabled" : "❌ Disabled"
);
console.log("  - Max Parallel Collections:", MAX_PARALLEL);
console.log(
  "  - Direct Transfer Mode:",
  USE_DIRECT_TRANSFER ? "✅ Enabled" : "❌ Disabled"
);
console.log("  - Batch Size:", BATCH_SIZE.toLocaleString());
console.log("📦 Collections to sync:", COLLECTIONS_TO_SYNC.join(", "));

// Utility function to extract database name from MongoDB URI
function extractDatabaseFromURI(uri) {
  try {
    const url = new URL(uri);
    return url.pathname.slice(1); // Remove leading slash
  } catch (error) {
    console.error("💥 Error parsing URI:", error.message);
    throw new Error(`Invalid MongoDB URI: ${uri}`);
  }
}

// Validate required environment variables
function validateConfig() {
  console.log("🔍 Validating configuration...");

  const missingVars = [];
  if (!REMOTE_URI) missingVars.push("REMOTE_URI");
  if (!LOCAL_URI) missingVars.push("LOCAL_URI");

  if (missingVars.length > 0) {
    console.error(
      "❌ Missing required environment variables:",
      missingVars.join(", ")
    );
    console.error("📝 Please set the following in your .env file:");
    console.error(
      `   REMOTE_URI="mongodb://USER:PASS@REMOTE_HOST:27017/SOURCE_DB"`
    );
    console.error(`   LOCAL_URI="mongodb://localhost:27017/TARGET_DB"`);
    process.exit(1);
  }

  // Extract and validate database names from URIs
  const sourceDB = extractDatabaseFromURI(REMOTE_URI);
  const targetDB = extractDatabaseFromURI(LOCAL_URI);

  console.log("✅ Configuration validated successfully");
  console.log(`📋 Extracted databases: ${sourceDB} → ${targetDB}`);

  return { sourceDB, targetDB };
}

// Function to execute command with logging and timeout
async function executeCommand(command, description, timeoutMs = 600000) {
  // 10 minutes default for individual collections
  console.log(`🔧 ${description}...`);
  console.log(`   Command: ${command}`);
  console.log(`   Timeout: ${timeoutMs / 1000}s`);

  const startTime = Date.now();

  try {
    const { stdout, stderr } = await execAsync(command, { timeout: timeoutMs });
    const executionTime = Date.now() - startTime;

    console.log(`✅ ${description} completed in ${executionTime}ms`);

    if (stdout.trim()) {
      console.log(`📤 Output: ${stdout.trim()}`);
    }

    if (stderr.trim()) {
      console.warn(`⚠️  Warning: ${stderr.trim()}`);
    }

    return { stdout, stderr, executionTime };
  } catch (error) {
    const executionTime = Date.now() - startTime;
    if (error.code === "TIMEOUT") {
      console.error(`⏰ ${description} timed out after ${timeoutMs / 1000}s`);
    } else {
      console.error(
        `💥 ${description} failed after ${executionTime}ms:`,
        error.message
      );
    }
    throw error;
  }
}

// Function to execute command with live progress
async function executeCommandWithProgress(
  command,
  description,
  timeoutMs = 600000
) {
  console.log(`🔧 ${description}...`);
  console.log(`   Command: ${command}`);
  console.log(`   Timeout: ${timeoutMs / 1000}s`);

  const startTime = Date.now();

  return new Promise((resolve, reject) => {
    const child = exec(command, { timeout: timeoutMs });

    let stdout = "";
    let stderr = "";

    child.stdout?.on("data", (data) => {
      stdout += data;
      // Show progress for mongodump operations
      if (data.includes("[") && data.includes("]")) {
        process.stdout.write(`   📊 ${data}`);
      }
    });

    child.stderr?.on("data", (data) => {
      stderr += data;
      // Show progress for mongodump operations
      if (data.includes("[") && data.includes("]")) {
        process.stdout.write(`   📊 ${data}`);
      }
    });

    child.on("close", (code) => {
      const executionTime = Date.now() - startTime;

      if (code === 0) {
        console.log(`\n✅ ${description} completed in ${executionTime}ms`);
        resolve({ stdout, stderr, executionTime });
      } else {
        console.error(
          `\n💥 ${description} failed with exit code ${code} after ${executionTime}ms`
        );
        reject(new Error(`Command failed with exit code ${code}`));
      }
    });

    child.on("error", (error) => {
      const executionTime = Date.now() - startTime;
      console.error(
        `\n💥 ${description} failed after ${executionTime}ms:`,
        error.message
      );
      reject(error);
    });
  });
}

// Optimized parallel execution
async function executeInParallel(tasks, maxConcurrency = MAX_PARALLEL) {
  const results = [];
  const executing = [];

  for (const task of tasks) {
    const promise = task().then((result) => {
      executing.splice(executing.indexOf(promise), 1);
      return result;
    });

    results.push(promise);
    executing.push(promise);

    if (executing.length >= maxConcurrency) {
      await Promise.race(executing);
    }
  }

  return Promise.all(results);
}

// Function to dump specific collections in parallel
async function dumpSpecificCollectionsParallel(tempDir, sourceDB) {
  console.log(
    `📥 Dumping ${COLLECTIONS_TO_SYNC.length} collections in parallel (max ${MAX_PARALLEL} concurrent)...`
  );

  const dumpTasks = COLLECTIONS_TO_SYNC.map(
    (collection, index) => () =>
      dumpSingleCollection(tempDir, collection, index + 1)
  );

  try {
    await executeInParallel(dumpTasks, MAX_PARALLEL);
    console.log(
      `\n🎉 All ${COLLECTIONS_TO_SYNC.length} collections dumped successfully!`
    );
  } catch (error) {
    console.error("❌ Parallel dump failed:", error.message);
    throw error;
  }
}

// Function to dump a single collection
async function dumpSingleCollection(tempDir, collection, index) {
  console.log(
    `\n📦 [${index}/${COLLECTIONS_TO_SYNC.length}] Starting dump: ${collection}`
  );

  const command = `mongodump --uri="${REMOTE_URI}" --collection="${collection}" --out="${tempDir}" --numParallelCollections=1`;

  try {
    await executeCommandWithProgress(command, `Dumping ${collection}`, 900000);
    console.log(
      `✅ [${index}/${COLLECTIONS_TO_SYNC.length}] Successfully dumped ${collection}`
    );
    return { collection, status: "success" };
  } catch (error) {
    console.error(
      `❌ [${index}/${COLLECTIONS_TO_SYNC.length}] Failed to dump ${collection}:`,
      error.message
    );
    throw error;
  }
}

// Function to restore collections in parallel
async function restoreSpecificCollectionsParallel(tempDir, sourceDB, targetDB) {
  console.log(
    `📤 Restoring ${COLLECTIONS_TO_SYNC.length} collections in parallel (max ${MAX_PARALLEL} concurrent)...`
  );

  const restoreTasks = COLLECTIONS_TO_SYNC.map(
    (collection, index) => () =>
      restoreSingleCollection(
        tempDir,
        sourceDB,
        targetDB,
        collection,
        index + 1
      )
  );

  try {
    await executeInParallel(restoreTasks, MAX_PARALLEL);
    console.log(
      `\n🎉 All ${COLLECTIONS_TO_SYNC.length} collections restored successfully!`
    );
  } catch (error) {
    console.error("❌ Parallel restore failed:", error.message);
    throw error;
  }
}

// Function to restore a single collection with optimizations
async function restoreSingleCollection(
  tempDir,
  sourceDB,
  targetDB,
  collection,
  index
) {
  console.log(
    `\n📦 [${index}/${COLLECTIONS_TO_SYNC.length}] Starting restore: ${collection}`
  );

  const sourcePath = path.join(tempDir, sourceDB, `${collection}.bson`);

  // Use optimized mongorestore with parallel processing
  const command = `mongorestore --uri="${LOCAL_URI}" --db="${targetDB}" --collection="${collection}" --drop --numParallelCollections=4 --numInsertionWorkersPerCollection=4 "${sourcePath}"`;

  try {
    await executeCommandWithProgress(
      command,
      `Restoring ${collection}`,
      900000
    );
    console.log(
      `✅ [${index}/${COLLECTIONS_TO_SYNC.length}] Successfully restored ${collection}`
    );
    return { collection, status: "success" };
  } catch (error) {
    console.error(
      `❌ [${index}/${COLLECTIONS_TO_SYNC.length}] Failed to restore ${collection}:`,
      error.message
    );
    throw error;
  }
}

// Direct database-to-database transfer (requires mongodb npm package)
async function directDatabaseTransfer(sourceDB, targetDB) {
  console.log("🔄 Starting direct database transfer (in-memory)...");

  try {
    const { MongoClient } = require("mongodb");

    const sourceClient = new MongoClient(REMOTE_URI);
    const targetClient = new MongoClient(LOCAL_URI);

    await sourceClient.connect();
    await targetClient.connect();

    const sourceDb = sourceClient.db(sourceDB);
    const targetDb = targetClient.db(targetDB);

    for (let i = 0; i < COLLECTIONS_TO_SYNC.length; i++) {
      const collection = COLLECTIONS_TO_SYNC[i];
      console.log(
        `\n📦 [${i + 1}/${
          COLLECTIONS_TO_SYNC.length
        }] Direct transfer: ${collection}`
      );

      await transferCollectionDirect(sourceDb, targetDb, collection);
      console.log(
        `✅ [${i + 1}/${
          COLLECTIONS_TO_SYNC.length
        }] Successfully transferred ${collection}`
      );
    }

    await sourceClient.close();
    await targetClient.close();

    console.log(
      `\n🎉 Direct transfer completed for all ${COLLECTIONS_TO_SYNC.length} collections!`
    );
  } catch (error) {
    if (error.code === "MODULE_NOT_FOUND") {
      console.error(
        "❌ MongoDB driver not found. Install with: npm install mongodb"
      );
      console.error("   Falling back to traditional method...");
      throw new Error("FALLBACK_TO_TRADITIONAL");
    }
    throw error;
  }
}

// Transfer single collection with batching
async function transferCollectionDirect(sourceDb, targetDb, collectionName) {
  const sourceCollection = sourceDb.collection(collectionName);
  const targetCollection = targetDb.collection(collectionName);

  // Drop target collection first
  try {
    await targetCollection.drop();
  } catch (err) {
    // Collection might not exist, which is fine
  }

  let processedCount = 0;
  const totalCount = await sourceCollection.countDocuments();
  console.log(
    `   📊 Total documents to transfer: ${totalCount.toLocaleString()}`
  );

  const cursor = sourceCollection.find({}).batchSize(BATCH_SIZE);
  let batch = [];

  for await (const doc of cursor) {
    batch.push(doc);

    if (batch.length >= BATCH_SIZE) {
      await targetCollection.insertMany(batch, { ordered: false });
      processedCount += batch.length;

      const progress = ((processedCount / totalCount) * 100).toFixed(1);
      process.stdout.write(
        `\r   📊 Progress: ${processedCount.toLocaleString()}/${totalCount.toLocaleString()} (${progress}%)`
      );

      batch = [];
    }
  }

  // Insert remaining documents
  if (batch.length > 0) {
    await targetCollection.insertMany(batch, { ordered: false });
    processedCount += batch.length;
  }

  console.log(
    `\n   ✅ Transferred ${processedCount.toLocaleString()} documents`
  );
}

// Function to dump specific collections from remote database (sequential)
async function dumpSpecificCollections(tempDir, sourceDB) {
  console.log(
    `📥 Dumping ${COLLECTIONS_TO_SYNC.length} specific collections...`
  );

  for (let i = 0; i < COLLECTIONS_TO_SYNC.length; i++) {
    const collection = COLLECTIONS_TO_SYNC[i];
    console.log(
      `\n📦 [${i + 1}/${
        COLLECTIONS_TO_SYNC.length
      }] Dumping collection: ${collection}`
    );
    console.log("=".repeat(60));

    const command = `mongodump --uri="${REMOTE_URI}" --collection="${collection}" --out="${tempDir}"`;

    try {
      await executeCommandWithProgress(
        command,
        `Dumping ${collection}`,
        900000
      ); // 15 minutes per collection
      console.log(`✅ Successfully dumped ${collection}`);
    } catch (error) {
      console.error(`❌ Failed to dump ${collection}:`, error.message);
      throw error;
    }
  }

  console.log(
    `\n🎉 All ${COLLECTIONS_TO_SYNC.length} collections dumped successfully!`
  );
}

// Function to restore specific collections to local database (sequential)
async function restoreSpecificCollections(tempDir, sourceDB, targetDB) {
  console.log(
    `📤 Restoring ${COLLECTIONS_TO_SYNC.length} specific collections...`
  );

  for (let i = 0; i < COLLECTIONS_TO_SYNC.length; i++) {
    const collection = COLLECTIONS_TO_SYNC[i];
    console.log(
      `\n📦 [${i + 1}/${
        COLLECTIONS_TO_SYNC.length
      }] Restoring collection: ${collection}`
    );
    console.log("=".repeat(60));

    const sourcePath = path.join(tempDir, sourceDB, `${collection}.bson`);
    const command = `mongorestore --uri="${LOCAL_URI}" --db="${targetDB}" --collection="${collection}" --drop "${sourcePath}"`;

    try {
      await executeCommandWithProgress(
        command,
        `Restoring ${collection}`,
        900000
      ); // 15 minutes per collection
      console.log(`✅ Successfully restored ${collection}`);
    } catch (error) {
      console.error(`❌ Failed to restore ${collection}:`, error.message);
      throw error;
    }
  }

  console.log(
    `\n🎉 All ${COLLECTIONS_TO_SYNC.length} collections restored successfully!`
  );
}

// Function to check if directory exists and create temp directory
async function setupTempDirectory() {
  const tempDir = "./temp-dump";
  console.log("📁 Setting up temporary directory...");

  try {
    // Remove existing temp directory if it exists
    try {
      await fs.access(tempDir);
      console.log("🗑️  Removing existing temp directory...");
      await fs.rm(tempDir, { recursive: true, force: true });
    } catch (error) {
      // Directory doesn't exist, which is fine
    }

    console.log("✅ Temporary directory prepared");
    return tempDir;
  } catch (error) {
    console.error("💥 Error setting up temp directory:", error);
    throw error;
  }
}

// Function to clean up temp directory
async function cleanupTempDirectory(tempDir) {
  console.log("🧹 Cleaning up temporary files...");

  try {
    const startTime = Date.now();
    await fs.rm(tempDir, { recursive: true, force: true });
    const cleanupTime = Date.now() - startTime;

    console.log(`✅ Temporary files cleaned up in ${cleanupTime}ms`);
  } catch (error) {
    console.warn(
      `⚠️  Warning: Could not clean up temp directory: ${error.message}`
    );
  }
}

// Function to verify specific collections
async function verifySpecificCollections(targetDB) {
  console.log("🔍 Verifying specific collections...");

  for (const collection of COLLECTIONS_TO_SYNC) {
    try {
      const command = `mongosh --uri="${LOCAL_URI}" --eval "db.getSiblingDB('${targetDB}').${collection}.countDocuments()" --quiet`;
      const { stdout } = await executeCommand(
        command,
        `Counting documents in ${collection}`,
        30000 // 30 seconds timeout for verification
      );

      const docCount = parseInt(stdout.trim());
      console.log(`✅ ${collection}: ${docCount.toLocaleString()} documents`);
    } catch (error) {
      console.warn(`⚠️  Could not verify ${collection}:`, error.message);
    }
  }
}

// Enhanced main execution function
async function main() {
  const scriptStartTime = Date.now();
  console.log("🎯 Starting optimized database synchronization...");

  let tempDir;

  try {
    // Validate configuration and extract database names
    const { sourceDB, targetDB } = validateConfig();

    let useTraditionalMethod = !USE_DIRECT_TRANSFER;

    if (USE_DIRECT_TRANSFER) {
      // Option 1: Direct database-to-database transfer (fastest)
      console.log("\n🚀 Mode: Direct Database Transfer (In-Memory)");
      console.log("=".repeat(70));

      try {
        await directDatabaseTransfer(sourceDB, targetDB);
      } catch (error) {
        if (error.message === "FALLBACK_TO_TRADITIONAL") {
          console.log("🔄 Falling back to traditional dump/restore method...");
          useTraditionalMethod = true;
        } else {
          throw error;
        }
      }
    }

    if (useTraditionalMethod) {
      // Option 2: Optimized dump/restore with parallel processing
      console.log("\n🚀 Mode: Optimized Dump/Restore with Parallel Processing");
      console.log("=".repeat(70));

      // Setup temp directory
      tempDir = await setupTempDirectory();

      if (USE_PARALLEL) {
        // Step 1: Parallel dump
        console.log("\n📥 Step 1: Parallel collection dumping");
        console.log("=".repeat(70));
        await dumpSpecificCollectionsParallel(tempDir, sourceDB);

        // Step 2: Parallel restore
        console.log("\n📤 Step 2: Parallel collection restoration");
        console.log("=".repeat(70));
        await restoreSpecificCollectionsParallel(tempDir, sourceDB, targetDB);
      } else {
        // Sequential processing (original method)
        console.log("\n📥 Step 1: Sequential collection dumping");
        console.log("=".repeat(70));
        await dumpSpecificCollections(tempDir, sourceDB);

        console.log("\n📤 Step 2: Sequential collection restoration");
        console.log("=".repeat(70));
        await restoreSpecificCollections(tempDir, sourceDB, targetDB);
      }
    }

    // Step 3: Verify sync (optional)
    if (SKIP_VERIFICATION) {
      console.log(
        "\n⏭️  Step 3: Skipping verification (SKIP_VERIFICATION=true)"
      );
    } else {
      console.log("\n✅ Step 3: Verifying collections");
      console.log("=".repeat(50));
      await verifySpecificCollections(targetDB);
    }

    const totalTime = Date.now() - scriptStartTime;
    console.log(
      "\n🎉 Optimized database synchronization completed successfully!"
    );
    console.log(
      `⏱️  Total execution time: ${totalTime}ms (${Math.round(
        totalTime / 1000
      )}s)`
    );
    console.log(`📊 Performance Summary:`);
    console.log(`   - Source: ${sourceDB} (remote)`);
    console.log(`   - Target: ${targetDB} (local)`);
    console.log(`   - Collections synced: ${COLLECTIONS_TO_SYNC.length}`);
    console.log(
      `   - Method: ${
        USE_DIRECT_TRANSFER
          ? "Direct Transfer"
          : USE_PARALLEL
          ? "Parallel Dump/Restore"
          : "Sequential Dump/Restore"
      }`
    );
    console.log(`   - Max Parallel: ${MAX_PARALLEL}`);
    console.log(`   - Batch Size: ${BATCH_SIZE.toLocaleString()}`);
    console.log(`   - Collections: ${COLLECTIONS_TO_SYNC.join(", ")}`);
  } catch (error) {
    console.error("💥 Optimized database synchronization failed:", error);
    console.error("🔍 Error details:", {
      name: error.name,
      message: error.message,
    });
    process.exit(1);
  } finally {
    // Always cleanup temp directory
    if (tempDir) {
      await cleanupTempDirectory(tempDir);
    }

    const totalTime = Date.now() - scriptStartTime;
    console.log(`🏁 Script finished at: ${new Date().toISOString()}`);
    console.log(
      `⏱️  Total script runtime: ${totalTime}ms (${Math.round(
        totalTime / 1000
      )}s)`
    );

    // Force exit to prevent hanging
    console.log("👋 Exiting...");
    process.exit(0);
  }
}

// Run the script
main();
