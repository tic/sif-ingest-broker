// Contains the definition of error classes
// used to handle/classify insertion failures.


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


module.exports = {
    PayloadError,
    NamingError
};
