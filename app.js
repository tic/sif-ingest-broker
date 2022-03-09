'use strict';


// Import custom packages
const { validateToken } = require("./cognito");
const { publicBroker } = require("./mqtt");
const { handler } = require("./handler");


// Runs every time a data ingest topic (data/#)
// receives a message. It's job is to authenticate
// the raw message and standardize the data into
// the Intermediate Representation (IR) format.
async function onMessageReceive(topic, message) {
    try {
        // Parse in the incoming message
        const jsonIn = JSON.parse(message.toString());

        // Was a token provided? Messages which
        // arrive through the ingest broker's
        // MQTT stream need identity tokens.
        if (!jsonIn.token) {
            throw new Error("no token provided");
        }

        // Attempt to validate the token. If it
        // is invalid, throw an appropriate error.
        const validation = await validateToken(jsonIn.token);
        if (validation.success === false) {
            throw new Error("Invalid token. Identity could not be verified. Is this a Cognito Identity token?");
        }

        // Sometimes a successful verification can
        // produce an empty username. Catch this
        // and don't invoke the handler.
        if (validation.username === null) {
            throw new Error("Empty username");
        }

        if(Array.isArray(jsonIn.data)) {
            jsonIn.data.forEach(data => {
                handler(
                    topic,
                    validation.username,
                    jsonIn.app_name,
                    data
                );
            });
        } else {
            // Invoke the SIF data handler with the topic,
            // username, and incoming JSON data blob.
            handler(
                topic,
                validation.username,
                jsonIn.app_name,
                jsonIn.data
            );
        }
    } catch(err) {
        console.error(err);
    }
}


// Attach message handler to the public broker.
publicBroker.on("message", onMessageReceive);


// Launch the custom source listener
const { launchCustomSourceListener } = require("./customSources");
launchCustomSourceListener();
