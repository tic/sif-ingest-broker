
// A transformer that does nothing
function passthrough(data) {
    return data;
}


// A transformer that removes certain characters from the data blob
function passthroughWithSafety(data) {
    var output = {
        app_name: data.app_name,
        time: data.time,
        metadata: {},
        payload_fields: {}
    };

    Object.keys(data.metadata).forEach(field => {
        const safeValue = data.metadata[field].replace(/[\s\-]+/g, "_");
        output.metadata[field] = safeValue;
    });

    Object.keys(data.payload_fields).forEach(field => {
        const safeField = field.replace(/[\s\-\(\)\?\=\*\&\^\%\$\#\@\!\<\>\/\,\.\;\'\"\{\}\[\]°]+/g, "_");
        output.payload_fields[safeField] = data.payload_fields[field];
    });

    return output;
}

//
//
//
//
//

const conversionMap = {
    "data/ingest/passthrough": passthrough,
    "data/ingest": passthroughWithSafety
}

function Transform(topic, data) {
    return conversionMap[topic](data);
}

module.exports = {
    Transform
};
