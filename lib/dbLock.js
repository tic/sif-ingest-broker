// Import semaphore construct
const { Semaphore } = require("async-mutex");


// Initialize semaphore
// --> Only 1 db access at a time
const dbSemaphore = new Semaphore(1);


// Define basic functions to simplify semaphore usage
var sValue, sRelease;
async function lockDb() {
    [sValue, sRelease] = await dbSemaphore.acquire();
}

async function unlockDb() {
    await sRelease();
}


// Export relevant functions
module.exports = {
    lock: lockDb,
    unlock: unlockDb
};