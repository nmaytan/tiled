# Client-Side Cache

## Overview

The client-side cache makes retrieving data faster, especially when it comes to large amounts of data. The cache integrates the Hishel library. If the cache is set in the transport (`Transport`), then when a request is intercepted by the transport it will check if the requested information is already in the cache prior to going to the server for the requested data. If the data is already stored in the cache, that is considered a cache hit and the data will be given to the user. Otherwise, that is a cache miss, and the data will have to be retrieved from the server.

## SQL Database Set Up

The SQL database is set up with two tables, one for the cache entries (`entries`) and one for the data that is being streamed (`streams`). Additionally, there is a table that contains the value of the cache version in case the cache gets updated in the future (this is used to ensure that the database is up to date with the version).

### Entries Table

The entries table consists of the following columns: `id`, `cache_key`, `data`, `created_at`, `deleted_at`, `size`, and `time_last_accessed`. The first 5 columns are created through Hishel, with the last two columns being added by Tiled to support necessary added functionality.

### Streams Table

The streams table consists of the following columns: `entry_id`, `chunk_number`, and `chunk_data`. The initialization of this table is entirely handled by Hishel.

## Size Constraints

The cache has size constraints when it comes to:
(1) how many bytes are present in the cache total
(2) how many bytes are in an individual entry.

The former can be controlled by the setting of the `capacity` variable and the latter through `max_item_size` at the time of the cache creation. For example, the following code will have a limit of 2 GB for entry sizes and 4 GB for the overall cache size:

```
cache = Cache(max_item_size=2_000_000_000, capacity=4_000_000_000)
```

The default max item size is 500,000, and the default capacity is 500,000,000. The units are bytes.

If the amount of items in the cache becomes too large and are over the capacity amount, an item will be evicted with the Least Recently Used (LRU) eviction method. This eviction method removes the entry that was accessed the longest time ago.

The size of each entry is contributed by anything that is put into the SQL table, including the cache key, ID, data, and metadata.

## Readonly Constraints

The client-side cache can be in readonly mode in which entries cannot be put into the cache and any entries that are already in the cache cannot be updated/modified (including the value of `time_last_accessed`, which is used for LRU eviction). The default is that the cache will not be readonly, however it can be set to be readonly as shown below:

```
cache = Cache(readonly=True)
```

## Networked Filesystem Constraints

The cache defaults to preventing the use of a networked filesystem for the location of the cache because they are unsafe for SQLite databases. This can be overrided by setting the environment variable `TILED_CLIENT_CACHE_AVOID_UNSAFE_FILESYSTEM` to `false`. This is not case-sensitive. If this environment variable is set to anything else, it will prevent the usage of networked filesystems as the location of the cache. It is highly recommended that the cache is not used at a networked filesystem location as the database used is SQLite and the file-locking mechanism may be corrupted and lead to unexpected behavior.

## Additional Cache Parameters

In addition to the aforementioned constraints, `Cache` has other parameters that can be used to customize values.

The `filepath` parameter can be used to set the location that the cache will be stored at. It defaults to the user's Tiled directory / "http_response_cache.db".

The `default_ttl` parameter, which stands for "Time to Live", determines how long an entry can be in the cache until it is deemed expired. Once a cached entry is considered expired, the next time that the automated cleanup runs that entry will be removed from the cache. The default value is `None`.

## Streaming

Data may be streamed through the client-side cache. This streaming feature is largely handled through Hishel, however the size of the streamed data is monitored within `Cache`. This size management is done through the use of a generator, `_check_max_stream_bytes`. This generator keeps track of the accumulated size as the stream is occurring and will remove the cached entry in the event that the size exceeds the maximum item size. If this happens, the entry is soft-deleted so that the stream may continue to provide data in the response (the only impact would be that the streamed data would not be cached, the stream will not be halted).

## Thread Safety

The client-side cache has methods in place to ensure thread safety through a thread lock to prevent parallel writes.

## Entry Size Constraint Enforcement

The function `measure_entry_size` is used as a pre-check to see if the size of the cached entry exceeds the maximum size of the item given by `max_item_size`. While the more reliable size check comes from `_check_max_stream_bytes`, this check isn't able to measure a chunk's size until it has been actually streamed. This is due to Hishel's lazy streaming method. So, an entry is made into the `entries` SQL table before the size check with `_check_max_stream_bytes` can happen. As a way to prevent an unnecessary write to the `entries` table, `measure_entry_size` is used to measure the size of the response and request based on the `"content-length"` value in their headers (if they exist). Since their existence depends on the server, `"content-length"` may not exist, but if they do then the size can be measured, and if the size is too large the write to `entries` would be avoided.

## Initializing the Database

Ensuring that the database is properly initialized is handled with the `_setup` function in Tiled which sets the `_setup_completed` Boolean variable and the `_initialized` Boolean variable which is used within Hishel. The reason for using both `_initialized` and `_setup_completed` is that the `_setup` function in Tiled also ensures that the proper version of Tiled cache is in use. Also, the `_initialize_database` function that is in Tiled must be run if the database is not initialized in order to add the additional `size` and `time_last_accessed` columns to the SQL table. `_initialized` must remain as well because that is how Hishel determines whether or not the database has been initialized, not with `_setup_completed`. Without it, the database may be attempted to be initialized multiple times.

## Connection Configuration

The connection is configured within Hishel at the initializing database stage. This configuration includes:

(1) `journal_mode` being set to `WAL` (Write-Ahead Logging). This setting allows readers and writers to access the database simultaneously.
(2) `busy_timeout` being set to `5000`. The units are milliseconds. This is how long a connection will retry when hitting a locked database. After that time is reached, `SQLITE_BUSY` is returned.
(3) `synchronous` being set to `NORMAL`. This makes it so it syncs at checkpoints (as opposed to every commit).
(4) `foreign_keys` being set to `ON`. This enables foreign key constraint enforcement to delete the children when the parent with the ID is deleted.

## Function Descriptions

### `create_entry` / `_create_entry`

Used to store an entry in the cache. `create_entry` ensures necessary requirements are present (such as the database being set up) and then calls `_create_entry` to actually store the entry in the cache. `_create_entry` will ensure that the size is within the constraint and enter the entry into the SQL table. It will do LRU eviction in the event the size of all entries in the cache exceeds the capacity of the cache. The function returns an Entry object in addition to writing to the SQL table. If the entry cannot be stored in the SQL table, the function will return a generic Entry object without commiting to the database.

### `get_entries`

The `get_entries` function is used to retrieve a response from the cache based on a provided key. This cache key is passed in as a parameter. The cache keys are set based on the function `_get_key_for_request` found in the `SyncCacheProxy` class of Hishel in `_sync_cache.py`. `get_entries` gathers any entries in the cache that have a key matching with the passed in `key` parameter and return those entries as a list. If there are no entries with a matching `key`, the function returns an empty list. Additionally, the `time_last_accessed` value of the cache entries gathered are updated with the current time in the SQL table as long as the value of `readonly` is false.

### `update_entry`

The `update_entry` function updates the data and cache key of an entry in the SQL table based on passed in parameters. This skips over entries that have an unfinished stream to not disrupt the stream due to it being lazy.

### `clear`

The `clear` function can be used to remove all entries in both the `streams` and `entries` SQL tables.

### `size` and `count` information

The `size` and `count` functions are used to gather information for the cache in terms of the size of all of the cache entries combined and the number of cached entries.

## Expired Entries

Once the time that a cached entry has been present in the cache exceeds the set time to live (`default_ttl` or the metadata `"hishel_ttl"`) value the entry would be considered expired. The next time `_batch_cleanup` on Hishel occurs, the entry will be deleted.

## Soft Deletion

The `remove_entry` function from Hishel is used when removing entries from the cache. This soft deletes the cached entry by marking the entry in the table. The entry does not become immediately deleted but instead is deleted after one hour by `_batch_cleanup`. The reason for soft deleting the entries instead of hard (or immediately) deleting the entries is because of the lazy method Hishel uses for handling streaming chunks. By soft deleting, if a stream is still occurring it will have time to finish streaming before the stream gets disrupted when a response is going through to the user. If the entries were to be hard deleted, it would disrupt the stream. After the time passes, all the entries that have been marked with being soft deleted will be removed from the cache.

## Transport

### Overview

The transport that is intended to be used with the cache is `Transport`. A client can be set up with this transport as follows:

```
tiled_cache = Cache()

client = httpx.Client(transport=Transport(cache=tiled_cache))
```

Further parameters can be set in the `Transport` transport to further customize it, such as with `cacheable_methods` which determines which types of methods (such as `"GET"` or `"POST"`) can be cached. Additionally, the `shared` parameter value can be set, which defaults to `False`. `shared` determines if the cache is meant to serve multiple users (`True`) or if the cache should act as a private cache (`False`). If dealing with authenticated responses, `shared` must be set to `False` or caching will be blocked due to Hishel's usage of RFC 9111 standards.

The transport intercepts requests and checks whether or not the requested data is present in the cache. If the requested data is present in the cache, then the data is provided to the user without having to go further to get the data. If the data is not in the cache, then the data must be retrieved from the server.

### Additional Transport Parameters

A parameter that can be set during the initialization of `Transport` is `transport`. `transport` determines what base transport is being used, with the defaults being `httpx.HTTPTransport`. Additionally, with a `cache` parameter present, the `transport` is wrapped with `SyncCacheTransport` from Hishel, which allows Hishel's transport to carry out its method for handling requests, and it handles certain activities like writes.

Another parameter that can be set is `limits`. This parameter determines limits for client behaviors, such as the maximum number of concurrent connections.
