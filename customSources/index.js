// Contains logic to implement the custom
// source listener functionality of SIF.


// Import libraries and custom packages.
const mqtt = require("mqtt");
const db = require("../db");
const { handler } = require("../handler");


// "Bookmark" variables that track which sources
// are currently being tracked and whether the
// source list is currently being updated.
var nextSourceId = 0;
var sourceRefreshActive = false;


// Use a closure to wrap the message handler 
// with the source owner's username.
function generateTtnHandler(username, backupAppName) {
    return function(topic, message) {
        try {
            // Get a device name, if applicable, without adding random
            // things to the function scope.
            const device = (function() {
                const topicParts = topic.split("/");
                let i = 0;
                for(; (i < topicParts.length && topicParts[i] !== "devices"); i++) {}
                return topicParts[++i] ?? "";
            })();

            // Parse the message packet into a JSON
            const parsed = JSON.parse(message.toString());

            // For TTN applications, we can pull the application id
            // right out of the message packet. However, some users
            // might want a single TTN application to funnel data
            // into >1 SIF app. So, we first search the user-defined
            // payload area for an app name. If one was not provided,
            // we use the on TTN provides. In the case that this name
            // is unavailable for some reason, we use a backup app
            // called ttn-mqtt-X, where X is a globally unique number.
            const appName = parsed.uplink_message.decoded_payload.app_name
                            || parsed.end_device_ids.application_ids.application_id
                            || backupAppName;

            // TTN messages contain a received_at ISO timestamp. By
            // default, we're going to use that as the data timestamp.
            // However, this can be overridden by adding a `time` key
            // into the `decoded_payload` section of the packet.
            const isoTimestamp = parsed.uplink_message.decoded_payload.time
                                ?? parsed.received_at;

            // Invoke the SIF data handler with the
            // pre-processed incoming data.
            handler(
                "data/ingest/passthrough",
                username,
                appName,
                {
                    time: new Date(isoTimestamp).getTime() / 1000,
                    device: device,
                    metadata: parsed.uplink_message.decoded_payload.metadata ?? {},
                    payload: parsed.uplink_message.decoded_payload.payload ?? {}
                }
            );
            
        } catch(err) {
            console.error(err);
        }
    }
}


// Takes in a custom source object and configures/
// attaches the appropriate handler/listener.
function handleNewSource(source) {
    try {
        const debugAppId = `${source.username}_Source ID ${source.id}`;
        switch(source.type) {
            case "mqtt":
                console.log("handling custom mqtt connection");
                break;
            
            case "ttn-mqtt":
                console.log("Attaching listener for custom TTN-MQTT source");

                // Establish a connection with the broker
                const customTtnClient = mqtt.connect(`mqtt://${source.metadata.brokerURL}`, {
                    username: source.metadata.username,
                    password: source.metadata.password,
                    host: source.metadata.brokerURL,
                    port: source.metadata.port
                });

                // Subscibe to all data topics
                customTtnClient.subscribe("#", async (err) => {
                    if(err) {
                        console.error("Failed to connect to TTN-MQTT broker");
                        
                        // Log the error to the error table
                        switch(err.message) {
                            case "Connection closed":
                                if (
                                    await db.logError(
                                        debugAppId, 
                                        "unable to connect to TTN-MQTT broker", 
                                        ""
                                    ) === false
                                ) {
                                    console.error("Failed to log error:");
                                    console.error(err);
                                }
                                break;

                            default:
                                console.error("unhandled connection error", err);
                        }
                    }
                });

                // Attach a message handler to the broker.
                customTtnClient.on(
                    "message",
                    generateTtnHandler(
                        source.username, 
                        `ttn-mqtt-${source.id}`
                    )
                );
                break;
            
            default:
                console.warn("unknown custom source type %s", source.type);
        }
    } catch(err) {
        console.error(err);
    }
}


// Checks for new custom sources and, if any
// are found, set up their subscriptions.
async function refreshCustomSources() {
    // In the unlikely event that a single refresh takes > 5 minutes,
    // this simple logic will prevent basic duplication of the function.
    if(sourceRefreshActive) {
        return;
    }

    sourceRefreshActive = true;
    
    try {
        const sources = await db.fetchSources(nextSourceId);
        if(sources === false) {
            console.error("failed to fetch sources list");
            return;
        }
        
        // If sources were retrieved, attach listeners for the
        // new ones.
        if(sources.length > 0) {
            nextSourceId = sources[sources.length - 1].id + 1;

            // forEach is empirically faster when the operation
            // is not intended to modify the source array.
            sources.forEach(handleNewSource);
        }
    } finally {
        sourceRefreshActive = false;
    }
}


// Initializes the custom source listener.
function launchCustomSourceListener() {
    refreshCustomSources();
    setInterval(refreshCustomSources, 5 * 60 * 1000);
}


// Export the launching function.
module.exports = {
    launchCustomSourceListener
};
