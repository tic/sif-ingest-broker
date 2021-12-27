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


// Set up mqtt handlers
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
        if (/[\w\d]+_[\w\d]+/g.test(safeAppId)) {
            throw "Unsafe app id";
        }


        const hypertableCached = AppCache.get(safeAppId);
        const hypertableExists = false;
        if (!hypertableCached) {
            hypertableExists = await db.hypertableExists(safeAppId);
            if (!hypertableExists) {
                // Create the hypertable.
            }
        }

        // The table is now guaranteed to exist. This message should be,
        // treated as insert-able timeseries data ONLY IF the table was
        // already in existence. This is true if the hypertable is either
        // cached already or if the existence check came back positive.
        if (hypertableCached || hypertableExists) {
            const irData = Transform(topic, jsonIn.data);
            const forwardedPayload = {
                app_id: safeAppId,
                data: irData
            }

            ingestStream.publish(
                "ingest/stream",
                JSON.stringify(forwardedPayload)
            );
        }

        // Regardless of how we got here, the safeAppId should, at this
        // point, be cached. It remains cached for an hour. After this,
        // the system will consult with the hypertable once more.
        AppCache.put(safeAppId, true, 3600*1000);

    } catch (err) {
        console.error(err);
    }
}


// Attach handlers to mqtt
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
