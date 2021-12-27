const { Pool } = require("pg");
const { config } = require('./config');


//
const pool = new Pool({
    user: config.TS_USER,
    host: config.TS_HOST,
    database: config.TS_DATABASE,
    password: config.TS_PASSWD,
    port: config.TS_PORT
});


//
async function query(text, params) {
    // const start = Date.now();
    const res = await pool.query(text, params);
    // const duration = Date.now() - start;
    // console.log("executed query", { text, duration, rows: res.rowCount });
    return res;
}


//
const QUERY_EXISTS = `
SELECT EXISTS (
    SELECT FROM pg_tables
    WHERE
        schemaname='public'
        AND tablename=$1
);
`
async function tableExists(table) {
    const result = await query(QUERY_EXISTS, [table]);
    return result.rows[0] && result.rows[0].exists === true;
}


//
async function getClient() {
    const client = await pool.connect();
    const query = client.query;
    const release = client.release;
    // set a timeout of 5 seconds, after which we will log this client's last query
    const timeout = setTimeout(() => {
        console.error("A client has been checked out for more than 5 seconds!");
        console.error(`The last executed query on this client was: ${client.lastQuery}`);
    }, 5000);
    // monkey patch the query method to keep track of the last query executed
    client.query = (...args) => {
        client.lastQuery = args;
        return query.apply(client, args);
    };
    client.release = () => {
        // clear our timeout
        clearTimeout(timeout);
        // set the methods back to their old un-monkey-patched version
        client.query = query;
        client.release = release;
        return release.apply(client);
    };
    return client;
}


//
function createAppId(username, appName) {
    return `${username}_${app_name}`;
}


// 
async function constructHypertable(appId, schema) {
    try {
        // 1. Is the schema object properly constructed?
        //      - It should be an object containing two keys: metadata and payload
        //      - Each object should contain keys, representing column names, that
        //        map to either "DOUBLE PRECISION" or "TEXT"

        // 2. Create a table according to the provided schema

        // 3. Convert it into a hypertable

        // 4. Commit changes and close cursor (if necessary)

    } catch (err) {
        console.error(err);
        return false;
    }
}


module.exports = {
    query: query,
    createAppId: createAppId,
    constructHypertable: constructHypertable
};
