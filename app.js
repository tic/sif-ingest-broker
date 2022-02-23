'use strict';


// Load configuration variables
const { config } = require("./lib/config");


// Import libraries
const cogExp = require("cognito-express");
const mqtt = require("mqtt");
const MemoryCache = require("memory-cache").Cache;


// Import custom packages
const { Transform, TransformError } = require("./lib/transformer");
const { validate } = require("./lib/tokens");
const db = require("./lib/db");


// Initialize caches
const AppCache = new MemoryCache();
const MetricsCache = new MemoryCache();


// Initialize db lock
// --> usage: await dbLock.lock(); <--> await dbLock.unlock();
const dbLock = require("./lib/dbLock");


// Establish mqtt connections
const publicBroker = mqtt.connect(config.PUBLIC_BROKER);
const ingestStream = mqtt.connect(config.INGEST_STREAM);


// Initialize channel variable
var channel = 0;


// Create error classes
// Source: https://stackoverflow.com/questions/1382107/whats-a-good-way-to-extend-error-in-javascript/1382129#1382129
class PayloadError extends Error {
    constructor(message, appId, device) {
        super(message);
        this.appId = appId;
        this.device = device || '';
        this.name = "PayloadError";
    }
}
class NamingError extends Error {
    constructor(message, appId, device) {
        super(message);
        this.appId = appId;
        this.device = device || '';
        this.name = "NamingError";
    }
}


// Runs every time a data ingest topic (data/#)
// receives a message. It's job is to authenticate
// the raw message and standardize the data into
// the Intermediate Representation (IR) format.
async function onMessageReceive(topic, message) {
    try {
        //console.log(topic, message.toString());
        const jsonIn = JSON.parse(message.toString());

        if (!jsonIn.token) {
            throw new Error("no token provided");
        }

        const validation = await validate(jsonIn.token);
        if (validation.success === false) {

            throw new Error("Invalid token. Identity could not be verified. Is this a Cognito Identity token?");
        }

        if (validation.username === null) {
            throw new Error("Empty username");
        }

        if (!jsonIn.app_name || !jsonIn.data) {
            throw new PayloadError(
                "invalid blob",
                `${validation.username}_${jsonIn.app_name || ''}`,
                null
            );
        }

        const safeAppId = db.createAppId(validation.username, jsonIn.app_name);
        if (/.*[^\w\d]+.*/.test(safeAppId)) {
            throw new NamingError("unsafe app name", safeAppId, null);
        }

        const irData;
        try {
            irData = Transform(topic, jsonIn.data);
        } catch (err) {
            // If this isn't a TransformError, bubble up immediately
            if (!(err instanceof TransformError)) {
                throw err;
            }

            // Otherwise, add the appId first, then bubble up
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
                    schema.metadata[key] = Number.isFinite(value) ? "DOUBLE PRECISION" : "TEXT";
                }

                // 2. Call db.constructHypertable(safeAppId, schema);
                const created = await db.constructHypertable(safeAppId, schema);
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
            "ingest/stream/" + channel,
            JSON.stringify(forwardedPayload)
        );
        channel = !channel + 0;
        //console.log("published to stream broker");

    } catch (err) {
        if (
            error instanceof PayloadError
            || error instanceof NamingError
            || error instanceof TransformError
        ) {
            // Log the error to the error table
            const errorStr = err.toString().substring(7);
            const appId = err.appId;
            const device = err.device;
            if (!username || await db.logError(appId, errorStr, device) === false) {
                console.error("Failed to log error:");
                console.error(err);
            }
        } else {
            console.error(err);
        }
    }
}


// Attach handlers to mqtt brokers
publicBroker.on("connect", () => {
    console.log("Connected to the public broker");
});
publicBroker.on("message", onMessageReceive);

ingestStream.on("connect", () => {
    console.log("Connected to the ingest stream");
    publicBroker.subscribe("data/#", () => {
        console.log("Subscribed to all data routes on the public broker");
    });
});
