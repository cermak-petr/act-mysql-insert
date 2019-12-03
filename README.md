# mysql-insert

Apify actor for inserting crawler results into a remote MySQL table.

This actor fetches all results from a specified dataset (or as raw data) and inserts them into
a table in a remote MySQL database.

## Input

Input is a JSON object with the following properties:

```javascript
{
    "datasetId": "your_dataset_id"
    "connection": {
        "host"      : "host_name",
        "user"      : "user_name",
        "password"  : "user_password",
        "database"  : "database_name"
    },
    "table": "table_name",
    "proxyUrl": "http://groups-StaticUS3:YOUR_PASSWORD@proxy.apify.com:8000" // Optionally you can tunnel through static proxies to whitelist only these IPs,
    "rows": [ // Optionally, you can add raw data instead of datasetId
        {"column_1": "value_1", "column_2": "value_2"},
        {"column_1": "value_3", "column_2": "value_4"}
    ]
}
```

## Webhooks
Very often you want to run an image `mysql-insert` update after every run of your scraping/automation actor. [Webhooks](https://apify.com/docs/webhooks) are solution for this. The default `datasetId` will be passed automatically to the this actor's run so you don't need to set it up in the payload template (internally the actor transforms the `resource.defaultDatasetId` from the webhook into just `datasetId` for its own input).

I strongly recommend to **create a task** from this actor with predefined input that will not change in every run - the only changing part is usually `datasetId`. You will not need to fill up the payload template and your webhook URL will then look like:
`https://api.apify.com/v2/actor-tasks/<YOUR-TASK-ID>/runs?token=<YOUR_API_TOKEN>`
