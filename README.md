# VixTK - Little scripts to do things

## Script to fetch all records from solr in batches of 1000 and save to a file
### File: `solr_fetch.py`
### Usage:
```shell
uv run main.py
```
### Parameters:
- `BASE_URL`: The base URL of the Solr instance without core (e.g., `http://localhost:8983/solr`)
- `CORE_NAME`: The name of the Solr core to fetch records from (e.g., `my_core`)
- `BATCH_SIZE`: The number of records to fetch in each batch (default is 9000)
- `FIELDS`: The fields to fetch from Solr 
- `SORT_KEYS`: The keys to sort the records by (default is `id asc`)
### Output:
- The script will save the fetched records to a file named `<CORE_NAME>_all_records.json` in the current directory.


## (WIP) Script to fetch all records from elasticsearch in batches of 1000 and save to a file
### File: `elastic_fetch.py`
### Usage:
```shell
uv run elastic_fetch.py <TODO: parameters>
```

## Script to create index on elasticsearch, create user and assign role to the user
### File: `elasticsearch_manager.py`
### Usage:
#### Create index, user and assign role to the user
Check the version of elasticsearch you are using and use the corresponding version of the package `elasticsearch`. 
Command to check server ES version:
```bash
# Server version
curl -sk -u elastic:'<ELASTIC_PASSWORD>' https://195.169.89.231:9200 | jq '.version.number'
```

```shell
uv run elasticsearch_manager.py create-index-user \
  --host "localhost" \
  --index myindex \
  --username myindex_user \
  --user-password myindex_password \
  --elastic-password elastic_superuser_password
```
_Note_: the test might fail due to SSL related issues. There is separate indicator to check whether creation is successful or not. 

