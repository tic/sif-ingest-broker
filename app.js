'use strict';


// Load configuration variables
const { config } = require("./lib/config");


// Import libraries
const cogExp = require("cognito-express");
const mqtt = require("mqtt");
const MemoryCache = require("memory-cache").Cache;


// Import custom packages
const { Transform } = require("./lib/transformer");
// TODO: import db


// Initialize user authenticator
const CognitoExpress = new cogExp({
    region: config.REGION,
    cognitoUserPoolId: config.USERPOOLID,
    tokenUse: "access",
    tokenExpiration: 3600000
});


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
function onMessageReceive(topic, message) {
    try {
        console.log(topic, message.toString());
        const jsonIn = JSON.parse(message.toString());
        if (!jsonIn.app_name || !jsonIn.token || !jsonIn.data) {
            throw "Missing required property in inbound message";
        }

        const irData = Transform(topic, jsonIn.data);

        const forwardedPayload = {
            app_id: "gmf.0",
            data: irData
        }

        ingestStream.publish(
            "ingest/stream",
            JSON.stringify(forwardedPayload)
        );

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
