const sql = require('mysql');
const Apify = require('apify');
const _ = require('underscore');
const Promise = require('bluebird');

const createProxyTunnel = require('./handle-proxy-url.js');

// get all unique keys from an array of objects
function getAllKeys(results, start, length) {
    const keys = {};
    function saveKeys(result) {
        for (const key in result) {
            if (!keys[key]) { keys[key] = true; }
        }
    }

    const end = Math.min(start + length, results.length);
    for (let i = start; i < end; i++) { saveKeys(results[i]); }

    return Object.keys(keys);
}

// iterate items from dataset
const loadItems = async (datasetId, process, offset) => {
    const limit = 1000;
    if (!offset) { offset = 0; }
    console.log('starting to load from dataset');
    const newItems = await Apify.client.datasets.getItems({
        datasetId,
        offset,
        limit,
    });
    if (newItems && newItems.items && newItems.items.length > 0) {
        await process(newItems.items);
        await loadItems(datasetId, process, offset + limit);
    }
};

// check if row already exists
async function checkIfExists(poolQuery, attr, value, table) {
    const select = `SELECT ${attr} FROM ${table} WHERE ${attr} = ${value};`;
    return await poolQuery(select);
}

// create SQL insert for a range of objects in an array
async function createInsert(results, start, length, table, staticParam, poolQuery, existsAttr) {
    // pre-define static SQL insert parts
    const keys = getAllKeys(results, start, length);
    const spKeys = staticParam ? Object.keys(staticParam).join(', ') : null;
    const spValues = staticParam ? Object.values(staticParam).join(', ') : null;
    const keyString = keys.join(', ') + (spKeys ? `, ${spKeys}` : '');

    let valueStrings = '';
    // add row to the SQL insert
    function addValueString(result) {
        valueStrings += valueStrings.length > 0 ? ', (' : '(';
        _.each(keys, (key, index) => {
            let val;
            if (result[key]) {
                if (typeof result[key] === 'number') {
                    val = result[key];
                } else {
                    val = "'" + result[key].replace(/'/g, "''") + "'";
                }
            } else {
                val = 'NULL';
            }
            valueStrings += index > 0 ? (', ' + val) : val;
        });
        if (spValues) { valueStrings += `, ${spValues}`; }
        valueStrings += ')';
    }

    // loop through all results and create SQL insert rows
    const end = Math.min(start + length, results.length);
    for (let i = start; i < end; i++) {
        const result = results[i];
        if (existsAttr && result[existsAttr] !== undefined) {
            const exists = await checkIfExists(poolQuery, existsAttr, result[existsAttr], table);
            if (exists) { console.log(`object already exists, will not be inserted: ${  JSON.stringify(result)}`); } else { addValueString(result); }
        } else { addValueString(result); }
    }

    // combine the SQL insert
    return `INSERT INTO ${table} (${keyString}) VALUES ${valueStrings};`;
}

Apify.main(async () => {
    const rowSplit = process.env.MULTIROW ? Number(process.env.MULTIROW) : 10;

    // get Act input and validate it
    const input = await Apify.getInput();

    const datasetId = input.resource ? input.resource.defaultDatasetId : input.datasetId;

    const { rows, connection, table, staticParam, existsAttr, proxyUrl } = input;

    if (!datasetId && !rows) {
        throw new Error('Missing "datasetId" or "rows" in INPUT!');
    }
    if (typeof connection !== 'object' || !connection.host) {
        throw new Error('Missing "connection" attribute in INPUT" or it has wrong format');
    }
    if (!table) {
        return console.log('Missing "table" attribute in INPUT');
    }

    // Setting reasonable default (by Petr)
    connection.connectionLimit = 10;

    if (proxyUrl) {
        const { newHost, newPort } = await createProxyTunnel(proxyUrl, connection.host);;
        connection.host = newHost;
        connection.port = newPort;
        console.log(`New proxied connection details:`);
        console.dir(connection);
    }

    // insert all results to MySQL
    async function processResults(poolQuery, results) {
        for (let i = 0; i < results.length; i += rowSplit) {
            const insert = await createInsert(results, i, rowSplit, table, staticParam, poolQuery, existsAttr);
            console.log(insert);
            try {
                const records = await poolQuery(insert);
                console.dir(records);
            } catch (e) { console.log(e); }
        }
    }

    try {
        // connect to MySQL and promisify it's methods
        const pool = sql.createPool(connection);
        const poolQuery = Promise.promisify(pool.query, { context: pool });
        const poolEnd = Promise.promisify(pool.end, { context: pool });

        // loop through pages of results and insert them to MySQL

        if (datasetId) {
            await loadItems(datasetId, async (results) => {
                await processResults(poolQuery, results);
            });
        } else if (rows) {
            await processResults(poolQuery, rows);
        }

        // disconnect from MySQL
        await poolEnd();
    } catch (e) { console.log(e); }
});
