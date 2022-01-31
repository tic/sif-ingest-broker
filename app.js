'use strict';


// Load configuration variables
const { config } = require("./lib/config");


// Import libraries
const cogExp = require("cognito-express");
const mqtt = require("mqtt");
const MemoryCache = require("memory-cache").Cache;


// Import custom packages
const { Transform } = require("./lib/transformer");
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


// Runs every time a data ingest topic (data/#)
// receives a message. It's job is to authenticate
// the raw message and standardize the data into
// the Intermediate Representation (IR) format.
async function onMessageReceive(topic, message) {
    try {
        console.log(topic, message.toString());
        const jsonIn = JSON.parse(message.toString());
        if (!jsonIn.app_name || !jsonIn.token || !jsonIn.data) {
            throw "Missing required property in inbound message";
        }

        const validation = await validate(jsonIn.token);
        if (validation.success === false) {
            throw "Invalid token";
        }

        if (validation.username === null) {
            throw "Empty username";
        }

        const safeAppId = db.createAppId(validation.username, jsonIn.app_name);
        if (/[^\w\d]/.test(safeAppId)) {
            throw "Unsafe app id";
        }

        const irData = Transform(topic, jsonIn.data);

        const hypertableCached = AppCache.get(safeAppId);
        let hypertableExists = false;
        if (!hypertableCached) {
            console.log("[CACHE MISS] on app id %s", safeAppId);
            hypertableExists = await db.hypertableExists(safeAppId);
            if (!hypertableExists) {
                // Goal: create the hypertable.
                console.log("[HT] creating hypertable for app id %s", safeAppId);

                // 1. Build the schema from the input
                const schema = {
                    metadata: {}
                };
                for (const [key, value] of Object.entries(irData.metadata)) {
                    schema.metadata[key] = isNaN(parseFloat(value)) ? "TEXT" : "DOUBLE PRECISION";
                }

                // 2. Call db.constructHypertable(safeAppId, schema);
                const created = await db.constructHypertable(safeAppId, schema);
                if (!created) {
                    throw "Failed to create hypertable";
                }

                console.log("[HT] proceeding to data insertion");
            }
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
        console.log("published to stream broker");

        // Regardless of how we got here, the safeAppId should, at this
        // point, be cached. It remains cached for an hour. After this,
        // the system will consult with the hypertable once more.
        AppCache.put(safeAppId, true, 3600*1000);

    } catch (err) {
        console.error(err);
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
