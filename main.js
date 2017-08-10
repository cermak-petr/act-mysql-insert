const sql = require('mysql');
const Apify = require('apify');
const _ = require('underscore');
const Promise = require('bluebird');
 
// check if value is String
function isString(value){
    return (typeof value === 'string' || value instanceof String);
}

// get all unique keys from an array of objects
function getAllKeys(results, start, length){
    
    const keys = {};
    function saveKeys(result){
        for(const key in result){
            if(!keys[key]){keys[key] = true;}
        }
    }
    
    const end = Math.min(start + length, results.length);
    for(let i = start; i < end; i++){saveKeys(results[i]);}
    
    return Object.keys(keys);
}

// create SQL insert for a range of objects in an array
function createInsert(results, start, length, table, staticParam){
    
    // pre-define static SQL insert parts
    const keys = getAllKeys(results, start, length);
    const spKeys = staticParam ? Object.keys(staticParam).join(', ') : null;
    const spValues = staticParam ? Object.values(staticParam).join(', ') : null;
    const keyString = keys.join(', ') + (spKeys ? ', ' + spKeys : '');
    
    let valueStrings = '';
    // add row to the SQL insert
    function addValueString(result){
        valueStrings += valueStrings.length > 0 ? ', (' : '(';
        _.each(keys, function(key, index){
            let val;
            if(result[key]){
                if(typeof result[key] === 'number'){val = result[key];}
                else{val = "'" + result[key].replace(/'/g, "''") + "'";}
            }
            else{val = 'NULL';}
            valueStrings += index > 0 ? (', ' + val) : val;
        });
        if(spValues){valueStrings += ', ' + spValues;}
        valueStrings += ')';
    }
    
    // loop through all results and create SQL insert rows
    const end = Math.min(start + length, results.length);
    for(let i = start; i < end; i++){
        addValueString(results[i]);
    }
    
    // combine the SQL insert
    return `INSERT INTO ${table} (${keyString}) VALUES ${valueStrings};`;
}

Apify.main(async () => {
    Apify.setPromisesDependency(Promise);
    const rowSplit = process.env.MULTIROW ? parseInt(process.env.MULTIROW) : 10;
    
    // get Act input and validate it
    const input = await Apify.getValue('INPUT');
    const data = input.data ? (typeof input.data === 'string' ? JSON.parse(input.data) : input.data) : {};
    if(!input._id){
        return console.log('missing "_id" attribute in INPUT');
    }
    if(!data.connection){
        return console.log('missing "connection" attribute in INPUT.data');
    }
    data.connection.connectionLimit = 10;
    if(!data.table){
        return console.log('missing "table" attribute in INPUT.data');
    }
    
    // set global executionId
    Apify.client.setOptions({executionId: input._id});
    
    // insert all results to MySQL
    async function processResults(poolQuery, lastResults){
        const results = _.chain(lastResults.items).pluck('pageFunctionResult').flatten().value();
        for(let i = 0; i < results.length; i += rowSplit){
            const insert = createInsert(results, i, rowSplit, data.table, data.staticParam);
            console.log(insert);
            try{
                const records = await poolQuery(insert);
                console.dir(records);
            }
            catch(e){console.log(e);}
        }
    }
    
    try{
        // connect to MySQL and promisify it's methods
        const pool = sql.createPool(data.connection);
        const poolQuery = Promise.promisify(pool.query, {context: pool});
        const poolEnd = Promise.promisify(pool.end, {context: pool});
        
        // loop through pages of results and insert them to MySQL
        const limit = 200;
        let total = null, offset = 0;
        while(total === null || offset + limit < total){
            const lastResults = await Apify.client.crawlers.getExecutionResults({limit: limit, offset: offset});
            await processResults(poolQuery, lastResults);
            total = lastResults.total;
        }
        
        // disconnect from MySQL
        await poolEnd();
    }
    catch(e){console.log(e);}
});