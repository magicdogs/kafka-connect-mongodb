
# Introduction

This connector is a wrapper for the MongoDb Async Driver.



## MongoDbSinkConnector

| Name                                            | Description                                                                                                                     | Type    | Default      | Valid Values                                                                                                                                        | Importance |
|-------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|---------|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|------------|
| mongodb.hosts                                   | MongoDb hosts to connect to.                                                                                                    | list    |              |                                                                                                                                                     | high       |
| mongodb.ssl.allow.invalid.hostnames             | Flag to determine if invalid hostnames should be allowed.                                                                       | boolean | false        |                                                                                                                                                     | high       |
| mongodb.ssl.enable                              | Flag to detemine if SSL configuration should be enabled.                                                                        | boolean | true         |                                                                                                                                                     | high       |
| mongodb.write.concern                           | mongodb.write.concern                                                                                                           | string  | ACKNOWLEDGED | ACKNOWLEDGED, FSYNCED, FSYNC_SAFE, JOURNALED, JOURNAL_SAFE, MAJORITY, NORMAL, REPLICAS_SAFE, REPLICA_ACKNOWLEDGED, SAFE, UNACKNOWLEDGED, W1, W2, W3 | medium     |
| mongodb.collection.expected.updates             | The number of expected writes per collection.                                                                                   | int     | 4096         | [512,...]                                                                                                                                           | low        |
| mongodb.database.cache.refresh.interval.seconds | The number of seconds to store the MongoDatabase and MongoCollection objects in cache before refreshing them from the database. | long    | 600          | [1,...]                                                                                                                                             | low        |



## Running in development

### Running 
```
./bin/debug.sh
```

### Suspending 

```
export SUSPEND='y'
./bin/debug.sh
```