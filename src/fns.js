const Apify = require('apify');
const sql = require('mysql2/promise');
const { createTunnel } = require('proxy-chain');
const bluebird = require('bluebird');
const mysqlString = require('sqlstring');

const { log } = Apify.utils;

/**
 * Loads items from one or many datasets in parallel by chunking the items from each dataset into batches,
 * retaining order of both items and datasets. Useful for large loads.
 * By default returns one array of items in order of datasets provided.
 * By changing concatItems or concatDatasets options, you can get array of arrays (of arrays) back
 * Requires bluebird dependency and copy calculateLocalOffsetLimit function!!!
 *
 * @param {string[]} datasetIds IDs or names of datasets you want to load
 * @param {object} options Options with default values.
 * If both concatItems and concatDatasets are false, output of this function is an array of datasets containing arrays
 * of batches containig array of items.
 * concatItems concats all batches of one dataset into one array of items.
 * concatDatasets concat all datasets into one array of batches
 * Using both concatItems and concatDatasets gives you back a sinlge array of all items in order.
 * Both are true by default.
 * @param {(items: any[], params: { datasetId: string, datasetOffset: number }) => Promise<void>} [options.processFn]
 *  Data are not returned by fed to the supplied async function on the fly (reduces memory usage)
 * @param {number} [options.parallelLoads]
 * @param {number} [options.batchSize]
 * @param {number} [options.offset=0]
 * @param {number} [options.limit=999999999]
 * @param {boolean} [options.concatItems]
 * @param {boolean} [options.concatDatasets]
 * @param {string[]} [options.fields]
 * @param {boolean} [options.useLocalDataset] Datasets will be always loaded from Apify could, even locally
 * @param {boolean} [options.debugLog]
 * @param {boolean} [options.persistLoadingStateForProcesFn=false]
 * @param {ReturnType<Apify.newClient>} [options.client]
 * Will not load batches that were already processed before migration, does nothing if processFn is not used.
 * It does not persist the state inside processFn, that is a responsibillity of the caller (if needed)
 * You must not manipulate input parameters (and underlying datasets) between migrations or this will break
 */
async function loadDatasetItemsInParallel(datasetIds, options = {}) {
    const {
        processFn,
        parallelLoads = 20,
        batchSize = 50000,
        offset = 0,
        limit = 999999999,
        concatItems = true,
        concatDatasets = true,
        debugLog = false,
        persistLoadingStateForProcesFn = false,
        fields,
        client = Apify.newClient(),
        // Figure out better name since this is useful for datasets by name on platform
        useLocalDataset = false, // Will fetch/create datasets by id or name locally or on current account
    } = options;

    if (!Apify.isAtHome() && useLocalDataset && fields) {
        log.warning(
            'loadDatasetItemsInParallel - fields option does not work on local datasets',
        );
    }

    const loadStart = Date.now();

    // Returns either null if offset/limit does not fit the current chunk
    // or { offset, limit } object
    const calculateLocalOffsetLimit = ({
        offset,
        limit,
        localStart,
        batchSize,
    }) => {
        const localEnd = localStart + batchSize;
        const inputEnd = offset + limit;

        // Offset starts after the current chunk
        if (offset >= localEnd) {
            return null;
        }
        // Offset + limit ends before our chunk
        if (inputEnd <= localStart) {
            return null;
        }

        // Now we know that the some data are in the current batch
        const calculateLimit = () => {
            // limit overflows current batch
            if (inputEnd >= localEnd) {
                // Now either the offset is less than local start and we do whole batch
                if (offset < localStart) {
                    return batchSize;
                }
                // Or it is inside the current batch and we slice it from the start (including whole batch)
                return localEnd - offset;
                // eslint-disable-next-line no-else-return
            } else {
                // Consider (inputEnd < localEnd) Means limit ends inside current batch
                if (offset < localStart) {
                    return inputEnd - localStart;
                }
                // This means both offset and limit are inside current batch
                return inputEnd - offset;
            }
        };

        return {
            offset: Math.max(localStart, offset),
            limit: calculateLimit(),
        };
    };

    // If we use processFnLoadingState, we skip requests that are done
    const createRequestArray = async (processFnLoadingState) => {
        // We increment for each dataset so we remember their order
        let datasetIndex = 0;

        // This array will be used to create promises to run in parallel
        const requestInfoArr = [];

        for (const datasetId of datasetIds) {
            if (processFnLoadingState && !processFnLoadingState[datasetId]) {
                processFnLoadingState[datasetId] = {};
            }
            // We get the number of items first and then we precreate request info objects
            let itemCount;
            if (useLocalDataset) {
                const dataset = await Apify.openDataset(datasetId);
                itemCount = await dataset
                    .getInfo()
                    .then((res) => res.itemCount);
            } else {
                itemCount = await client
                    .dataset(datasetId)
                    .get()
                    .then((res) => res.itemCount);
            }
            if (debugLog) {
                log.info(`Dataset ${datasetId} has ${itemCount} items`);
            }
            const numberOfBatches = Math.ceil(itemCount / batchSize);

            for (let i = 0; i < numberOfBatches; i++) {
                const localOffsetLimit = calculateLocalOffsetLimit({
                    offset,
                    limit,
                    localStart: i * batchSize,
                    batchSize,
                });
                if (!localOffsetLimit) {
                    continue; // eslint-disable-line no-continue
                }

                if (processFnLoadingState) {
                    if (
                        !processFnLoadingState[datasetId][
                            localOffsetLimit.offset
                        ]
                    ) {
                        processFnLoadingState[datasetId][
                            localOffsetLimit.offset
                        ] = { done: false };
                    } else if (
                        processFnLoadingState[datasetId][
                            localOffsetLimit.offset
                        ].done
                    ) {
                        log.info(
                            `Batch for dataset ${datasetId}, offset: ${localOffsetLimit.offset} was already processed, skipping...`,
                        );
                        continue; // eslint-disable-line no-continue
                    }
                }

                requestInfoArr.push({
                    index: i,
                    offset: localOffsetLimit.offset,
                    limit: localOffsetLimit.limit,
                    datasetId,
                    datasetIndex,
                });
            }

            datasetIndex++;
        }
        return requestInfoArr;
    };

    // This is array of arrays. Top level array is for each dataset and inside one entry for each batch (in order)
    /** @type {any[]} */
    let loadedBatchedArr = [];

    let totalLoaded = 0;
    const totalLoadedPerDataset = {};

    const processFnLoadingState = persistLoadingStateForProcesFn
        ? (await Apify.getValue('PROCESS-FN-LOADING-STATE')) || {}
        : null;

    // Apify.events doesn't work because this is different Apify instance
    if (processFnLoadingState) {
        setInterval(async () => {
            await Apify.setValue(
                'PROCESS-FN-LOADING-STATE',
                processFnLoadingState,
            );
        }, 15000);
    }

    const requestInfoArr = await createRequestArray(processFnLoadingState);
    if (debugLog) {
        log.info(`Number of requests to do: ${requestInfoArr.length}`);
    }

    //  Now we execute all the requests in parallel (with defined concurrency)
    await bluebird.map(
        requestInfoArr,
        async (requestInfoObj) => {
            const { index, datasetId, datasetIndex } = requestInfoObj;

            const getDataOptions = {
                offset: requestInfoObj.offset,
                limit: requestInfoObj.limit,
                fields,
            };
            let items;
            if (useLocalDataset) {
                // This open should be cached
                const dataset = await Apify.openDataset(datasetId);

                if (!Apify.isAtHome()) {
                    delete getDataOptions.fields;
                }
                items = await dataset
                    .getData(getDataOptions)
                    .then((res) => res.items);
            } else {
                items = await client
                    .dataset(datasetId)
                    .listItems(getDataOptions)
                    .then((res) => res.items);
            }

            if (!totalLoadedPerDataset[datasetId]) {
                totalLoadedPerDataset[datasetId] = 0;
            }

            totalLoadedPerDataset[datasetId] += items.length;
            totalLoaded += items.length;

            if (debugLog) {
                log.info(
                    `Items loaded from dataset ${datasetId}: ${items.length}, offset: ${requestInfoObj.offset},
        total loaded from dataset ${datasetId}: ${totalLoadedPerDataset[datasetId]},
        total loaded: ${totalLoaded}`,
                );
            }
            // We either collect the data or we process them on the fly
            if (processFn) {
                await processFn(items, {
                    datasetId,
                    datasetOffset: requestInfoObj.offset,
                });
                if (processFnLoadingState) {
                    processFnLoadingState[datasetId][
                        requestInfoObj.offset
                    ].done = true;
                }
            } else {
                if (!loadedBatchedArr[datasetIndex]) {
                    loadedBatchedArr[datasetIndex] = [];
                }
                // Now we correctly assign the items into the main array
                loadedBatchedArr[datasetIndex][index] = items;
            }
        },
        { concurrency: parallelLoads },
    );

    if (debugLog) {
        log.info(
            `Loading took ${Math.round(
                (Date.now() - loadStart) / 1000,
            )} seconds`,
        );
    }

    if (processFnLoadingState) {
        await Apify.setValue('PROCESS-FN-LOADING-STATE', processFnLoadingState);
    }

    if (!processFn) {
        if (concatItems) {
            for (let i = 0; i < loadedBatchedArr.length; i++) {
                /**
                 * @param {any} item
                 */
                loadedBatchedArr[i] = loadedBatchedArr[i].flatMap(
                    (item) => item,
                );
            }
        }

        if (concatDatasets) {
            loadedBatchedArr = loadedBatchedArr.flatMap((item) => item);
        }
        return loadedBatchedArr;
    }
}

/**
 *
 * @param {string} proxyUrl
 * @param {string} host
 * @param {string} port
 */
async function createProxyTunnel(proxyUrl, host, port = '3306') {
    /** @type {string} */
    const tunnel = await createTunnel(proxyUrl, `${host}:${port}`);
    log.info(`Created tunnel: ${tunnel}`);

    return tunnel.split(':', 2)[1];
}

/**
 * get all unique keys from an array of objects
 *
 * @param {Array<Record<string, any>>} results
 * @param {number} start
 * @param {number} length
 */
function getAllKeys(results, start, length) {
    const keys = new Set();

    function saveKeys(result) {
        for (const key of Object.keys(result)) {
            if (!key.startsWith('#')) {
                keys.add(key);
            }
        }
    }

    const end = Math.min(start + length, results.length);
    for (let i = start; i < end; i++) {
        saveKeys(results[i]);
    }

    return [...keys.values()];
}

/**
 * check if row already exists
 *
 * @param {sql.Pool} pool
 * @param {string} attr
 * @param {string} value
 * @param {string} table
 */
async function checkIfExists(pool, attr, value, table) {
    const select = mysqlString.format(
        `SELECT ${mysqlString.escapeId(attr)} FROM ${mysqlString.escapeId(table)} WHERE ${mysqlString.escapeId(attr)} = ?;`,
        [value],
    );
    return pool.query(select);
}

/**
 * create SQL insert for a range of objects in an array
 *
 * @param {any[]} results
 * @param {number} start
 * @param {number} length
 * @param {string} table
 * @param {Record<string, any>} staticParam
 * @param {sql.Pool} pool
 * @param {string} existsAttr
 */
async function createInsert(
    results,
    start,
    length,
    table,
    staticParam,
    pool,
    existsAttr,
) {
    // pre-define static SQL insert parts
    const keys = getAllKeys(results, start, length);
    const spKeys = staticParam ? Object.keys(staticParam) : [];
    const spValues = staticParam ? Object.values(staticParam).map((s) => mysqlString.escape(s, true)) : [];
    const keyString = [...keys, ...spKeys].map((key) => mysqlString.escapeId(key)).join(',');

    /** @type {string[]} */
    const values = [];
    // add row to the SQL insert
    function addValueString(result) {
        /** @type {string[]} */
        const toAdd = [];

        for (const key of keys) {
            toAdd.push(mysqlString.escape(result[key], true));
        }

        if (spValues?.length) {
            toAdd.push(...spValues);
        }

        values.push(toAdd.join(','));
    }

    // loop through all results and create SQL insert rows
    const end = Math.min(start + length, results.length);

    for (let i = start; i < end; i++) {
        const result = results[i];

        if (existsAttr && result[existsAttr] !== undefined) {
            const exists = await checkIfExists(
                pool,
                existsAttr,
                result[existsAttr],
                table,
            );

            if (exists) {
                log.warning(
                    `object already exists, will not be inserted`,
                    result,
                );
            } else {
                addValueString(result);
            }
        } else {
            addValueString(result);
        }
    }

    if (!values?.length) {
        // no values to insert in case every value exists, just return
        return;
    }

    // combine the SQL insert
    return `INSERT INTO ${table} (${keyString}) VALUES (${values.join('),(')});`;
}

module.exports = {
    createInsert,
    createProxyTunnel,
    checkIfExists,
    loadDatasetItemsInParallel,
};
