// Creates the default MQTT broker connections
// to the public and stream brokers.


// Load the application config
const { config } = require("../config");


// Import the MQTT package.
const mqtt = require("mqtt");


// Establish mqtt connections
const publicBroker = mqtt.connect(config.PUBLIC_BROKER);
const ingestStream = mqtt.connect(config.INGEST_STREAM);


// Attach listener to log when a connection
// to the public broker has been established.
publicBroker.on("connect", () => {
    console.log("Connected to the public broker");
});


// Attach listener to connect to the stream broker
// and, once connected, start listening for messages
// on the ingest broker's data topics.
ingestStream.on("connect", () => {
    console.log("Connected to the ingest stream");
    publicBroker.subscribe("data/#", () => {
        console.log("Subscribed to all data routes on the public broker");
    });
});


// Export the broker connections.
module.exports = {
    publicBroker,
    ingestStream
};