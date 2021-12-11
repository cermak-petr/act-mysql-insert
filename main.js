const sql = require('mysql2/promise');
const Apify = require('apify');
const { createInsert, createProxyTunnel, loadDatasetItemsInParallel } = require('./src/fns');

const { log } = Apify.utils;

Apify.main(async () => {
    /** @type {any} */
    const input = await Apify.getInput();

    const datasetId = input.resource?.defaultDatasetId ? input.resource.defaultDatasetId : input.datasetId;

    const { rows, connection, rowSplit = 10, table, staticParam, existsAttr, proxyConfig, debugLog = false } = input;

    if (!datasetId && !rows) {
        throw new Error('Missing "datasetId" or "rows" in INPUT!');
    }

    if (!connection?.host) {
        throw new Error('Missing "connection" attribute in INPUT" or it has wrong format');
    }

    if (!table) {
        throw new Error('Missing "table" attribute in INPUT');
    }

    if (debugLog === true) {
        log.setLevel(log.LEVELS.DEBUG);
    }

    const proxy = await Apify.createProxyConfiguration(proxyConfig);

    // Setting reasonable default (by Petr)
    connection.connectionLimit = 10;

    if (proxy?.usesApifyProxy || proxy?.proxyUrls?.length) {
        const proxyUrl = proxy.newUrl();
        const newPort = await createProxyTunnel(proxyUrl, connection.host, connection.port);
        connection.host = 'localhost';
        connection.port = newPort;
        log.info(`New proxied connection details`, connection);
    }

    /**
     * insert all results to MySQL
     *
     * @param {sql.Pool} pool
     * @param {Array<Record<string, any>>} results
     */
    async function processResults(pool, results) {
        for (let i = 0; i < results.length; i += rowSplit) {
            const insert = await createInsert(results, i, rowSplit, table, staticParam, pool, existsAttr);

            if (!insert) {
                continue;
            }

            log.debug('insert', { insert });

            try {
                const records = await pool.query(insert);

                log.debug('records', { records });
            } catch (e) {
                log.exception(e, 'processResults');
            }
        }
    }

    try {
        // connect to MySQL and promisify it's methods
        const pool = sql.createPool(connection);

        // loop through pages of results and insert them to MySQL

        if (datasetId) {
            await loadDatasetItemsInParallel([datasetId], {
                persistLoadingStateForProcesFn: true,
                processFn: async (results) => {
                    await processResults(pool, results);
                },
            });
        } else if (rows?.length) {
            await processResults(pool, rows);
        } else {
            log.warning('Doing nothing');
        }

        // disconnect from MySQL
        await pool.end();
    } catch (e) {
        log.exception(e, 'Failed to insert');
    }
});
