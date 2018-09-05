# act-mysql-insert

Apify act for inserting crawler results into a remote MySQL table.

This act fetches all results from a specified Apifier crawler execution and inserts them into
a table in a remote MySQL database.

The act does not store its state, i.e. if it crashes it restarts fetching all the results.
Therefore you should only use it for executions with low number of results.


**INPUT**

Input is a JSON object with the following properties:

```javascript
{
    // crawler executionID
    "_id": "your_execution_id",

    // MySQL connection credentials
    "data": {
        "connection": {
          "host"      : "host_name",
          "user"      : "user_name",
          "password"  : "user_password",
          "database"  : "database_name"
        },
        "table": "table_name"
    }
}
```

__The act can be run with a crawler finish webhook, in such case fill just the contents of data 
attribute into a crawler finish webhook data.__

Alternatively you can directly specify the rows to be inserted (i.e. not fetching them from crawler execution).
```javascript
{
    // rows to be inserted
    "rows": [
        {"column_1": "value_1", "column_2": "value_2"},
        {"column_1": "value_3", "column_2": "value_4"},
        ...
    ],

    // PostgreSQL connection credentials
    "data": "connection_credentials"
}
```
