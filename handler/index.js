
// Import libraries and custom packages.
const db = require("../db");
const { ingestStream } = require("../mqtt");
const { Transform, TransformError } = require("../transform");
const MemoryCache = require("memory-cache").Cache;


// Initialize cache. We use the cache for
// tracking whether app tables already exist
// and avoiding an expensive (time) check.
const AppCache = new MemoryCache();


// Initialize db lock
// --> usage: await dbLock.lock(); <--> await dbLock.unlock();
const dbLock = require("../db/dbLock");


// Until true multi-client MQTT is supported by
// the stream broker, we use a simple technique
// to split the data between two ingest channels.
var publishingChannel = 0;


// Import error classes
const { PayloadError, NamingError } = require("../errors");


// SIF message handler
async function handler(topic, username, appName, data) {
    try {
        console.log(
            topic,
            username,
            appName,
            data
        );
        if (!appName || !data) {
            throw new PayloadError(
                "invalid blob",
                `${username}_${appName || ''}`,
                null
            );
        }

        const safeAppId = db.createAppId(username, appName);
        if (/.*[^\w\d-]+.*/.test(safeAppId)) {
            throw new NamingError("unsafe app name", safeAppId, null);
        }

        let irData;
        try {
            irData = Transform(topic, data);
        } catch (err) {
            // If this isn't a TransformError, bubble up immediately.
            if (!(err instanceof TransformError)) {
                throw err;
            }

            // Otherwise, add the appId first, then bubble up.
            err.appId = safeAppId;
            throw err;
        }

        const hypertableCached = AppCache.get(safeAppId);
        let hypertableExists = false;
        if (!hypertableCached) {
            console.log("[CACHE MISS] on app id %s", safeAppId);
            await dbLock.lock();
            hypertableExists = await db.hypertableExists(safeAppId);
            if (!hypertableExists) {
                // Goal: create the hypertable.
                console.log("[HT] creating hypertable for app id %s", safeAppId);

                // 1. Build the schema from the input
                const schema = {
                    metadata: {}
                };
                for (const [key, value] of Object.entries(irData.metadata)) {
                    // Metadata strings can be up to 128 characters long.
                    // This limit is somewhat arbitrarily chosen, and is
                    // elligible for change, if desired. The main reason 
                    // for using a fixed-max-length varchar is because it 
                    // seems to require less overhead than the text type.
                    schema.metadata[key] = Number.isFinite(value) ? "DOUBLE PRECISION" : "VARCHAR(128)";
                }

                // If string data is provied in packet 0, recognize it.
                // Srting data is highlighted by seeking out metrics
                // which map to non-numerica data.
                const stringData = 
                    Object.entries(irData.payload)
                    .filter(([columnName, data]) => !Number.isFinite(data))
                    .map(([columnName, ]) => columnName);
                console.log("found string data: %s", stringData.toString());

                // 2. Call db.constructHypertable(safeAppId, schema);
                const created = await db.constructHypertable(safeAppId, schema, stringData);
                if (!created) {
                    throw "Failed to create hypertable";
                }

                console.log("[HT] proceeding to data insertion");
            }

            // safeAppId remains cached for an hour. After this,
            // the system will consult with the hypertable once more.
            AppCache.put(safeAppId, true, 3600 * 1000);
            console.log("[CACHED] app id %s", safeAppId);
            await dbLock.unlock();
        }

        // The table is now guaranteed to exist. This message should be,
        // treated as normal, insert-able, timeseries data now.
        const forwardedPayload = {
            app_id: safeAppId,
            data: irData
        }

        // Publish the necessary data to the stream broker
        ingestStream.publish(
            "ingest/stream/" + publishingChannel,
            JSON.stringify(forwardedPayload)
        );
        publishingChannel = !publishingChannel + 0;
        //console.log("published to stream broker");

    } catch (err) {
        if (
            err instanceof PayloadError
            || err instanceof NamingError
            || err instanceof TransformError
        ) {
            // Log the error to the error table
            console.log(err.toString());
            const errorStr = err.toString();
            const appId = err.appId;
            const device = err.device;
            if (
                !username 
                || await db.logError(
                    appId, 
                    errorStr
                        .substring(
                            errorStr.indexOf(": ") + 2
                        ), 
                    device) === false
            ) {
                console.error("Failed to log error:");
                console.error(err);
            }
        } else {
            console.error(err);
        }
    }
}

module.exports = {
    handler
};