# act-mysql-insert

Apify act for inserting crawler results into a remote MySQL table.

This act fetches all results from a specified Apifier crawler execution and inserts them into
a table in a remote MySQL database.

The act does not store its state, i.e. if it crashes it restarts fetching all URLs.
Therefore you should only use it for short lists of URLs.


**INPUT**

Input is a JSON object with the following properties:

```javascript
{
    "_id": "YOUR_EXECUTION_ID",
    "data": {
        "connection": "MSSQL_CONNECTION_STRING",
        "table": "DB_TABLE_NAME"
    }
}
```

__The act can be run with a crawler finish webhook, in such case fill just the contents of data 
attribute into a crawler finish webhook data.__
