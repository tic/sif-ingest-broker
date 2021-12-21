// Load environment variables
(require("dotenv")).config();
const config = process.env;

if (config.ENV === "DEVELOPMENT") {
    config.PUBLIC_BROKER = "mqtt://localhost";
    config.INGEST_STREAM = "mqtt://localhost";
}

module.exports = {
    config
};