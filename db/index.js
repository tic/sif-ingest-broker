// Import libraries
const { Pool } = require("pg");
const { parseSsl } = require("pg-ssl");
const format = require('pg-format');


// Load the configuration file
const { config } = require('../config');


// Create a connection pool to the database
const pool = new Pool({
    user: config.TS_USER,
    host: config.TS_HOST,
    database: config.TS_DATABASE,
    password: config.TS_PASSWD,
    port: config.TS_PORT,
    ssl: parseSsl()
});


// Executes the query given by @text using SQL
// parameters provided by @params.
async function query(text, params) {
    // const start = Date.now();
    const res = await pool.query(text, params);
    // const duration = Date.now() - start;
    // console.log("executed query", { text, duration, rows: res.rowCount });
    return res;
}


// Blank template query for determining whether a
// hypertable exists or not.
const QUERY_EXISTS = `
SELECT EXISTS (
    SELECT FROM pg_tables
    WHERE
        schemaname='public'
        AND tablename=$1
);
`


// Blank template for inserting a new error into
// the error table.
const QUERY_ERROR_INS = `
INSERT INTO "errorTable" (app_id, error, device)
VALUES ($1, $2, $3)
ON CONFLICT ON CONSTRAINT unique_error_src
DO
    UPDATE SET timestamp=NOW();
`;


// Blank template for retrieving the list of
// custom sources from the sources table.
const QUERY_CUSTOM_SOURCES = `
SELECT * 
FROM "sources" 
WHERE id>=$1 
ORDER BY id asc;
`;


// Given a table, returns a boolean value according
// to whether a hypertable with that name exists.
async function hypertableExists(table) {
    const result = await query(QUERY_EXISTS, [table]);
    return result.rows[0] && result.rows[0].exists === true;
}


// Given an app name and the username that wants to
// use it, return a globally unique app id. This is
// a pretty simple function here, but could be made
// more complex through a variety of means in order
// to achieve certain results.
function createAppId(username, appName) {
    return `${username}_${appName}`;
}


// Given a globally unique app id and a part of the
// table schema, create a table and convert it into
// a hypertable. Returns a boolean according to the
// success of these operations.
async function constructHypertable(appId, schema) {
    try {
        // 1. Create a table according to the provided schema
        const metadataColumns = [
            "time TIMESTAMPTZ NOT NULL",
            "metric TEXT NOT NULL",
            "value DOUBLE PRECISION NOT NULL"
        ];
        const parametersCreateTable = [appId]
        for (const [key, value] of Object.entries(schema.metadata)) {
            metadataColumns.push(`%I ${value}`);
            parametersCreateTable.push(key);
        }
        const queryCreateTable = format(
            `CREATE TABLE %I (` + metadataColumns.join(", ") + ");",
            ...parametersCreateTable
        );
        await query(queryCreateTable);

        // 2. Convert it into a hypertable
        const queryConversion = `SELECT create_hypertable('%I', 'time')`;
        await query(
            format(queryConversion, appId)
        );

        // 3. Enable compression
        const queryCompression = `ALTER TABLE %I SET(timescaledb.compress, timescaledb.compress_segmentby='metric')`
        await query(
            format(
                queryCompression,
                appId
            )
        );

        // 4. Add compression policy
        const queryCompressionPolicy = `SELECT add_compression_policy('%I', INTERVAL '4d')`;
        await query(
            format(
                queryCompressionPolicy,
                appId
            )
        );

        return true;
    } catch (err) {
        console.error(err);
        return false;
    }
}


// Adds an error to the error table and associates
// it with the user responsible.
async function logError(appId, error, device) {
    try {
        await query(
            QUERY_ERROR_INS,
            [appId, error, device || '']
        );
        return true;
    } catch (err) {
        console.error(err);
        return false;
    }
}


// Retrieves the list of custom sources from the
// source table.
async function fetchSources(minimumId) {
    const minId = minimumId ?? 0;
    try {
        const dbResponse = await query(
            QUERY_CUSTOM_SOURCES,
            [minId]
        );
        return dbResponse.rows;
    } catch(err) {
        console.error(err);
        return false;
    }
}


// Export necessary functions
module.exports = {
    createAppId: createAppId,
    constructHypertable: constructHypertable,
    hypertableExists: hypertableExists,
    fetchSources: fetchSources
};
