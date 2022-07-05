# Melior Service Harness :: Mongo
<div style="display: inline-block;">
<img src="https://img.shields.io/badge/version-2.3-green?style=for-the-badge"/>
<img src="https://img.shields.io/badge/production-ready-green?style=for-the-badge"/>
<img src="https://img.shields.io/badge/compatibility-spring_boot_2.4.5-green?style=for-the-badge"/>
</div>

## Artefact
Get the artefact and the POM file in the *artefact* folder.
```
<dependency>
    <groupId>org.melior</groupId>
    <artifactId>melior-harness-mongo</artifactId>
    <version>2.3</version>
</dependency>
```

## Client
Create a bean to instantiate the MongoDB client.  The MongoDB client uses connection pooling to improve performance.
```
@Bean("myclient")
@ConfigurationProperties("myclient")
public MongoClient client() {
    return MongoClientBuilder.create().build();
}
```

The MongoDB client is auto-configured from the application properties.
```
myclient.url=mongodb://some.service:27017
myclient.username=user
myclient.password=password
myclient.database=myDatabase
myclient.request-timeout=30
myclient.inactivity-timeout=15
```

Wire in and use the MongoDB client.
```
@Autowired
@Qualifier("myclient")
private MongoClient client;

public void foo(Person person) throws RemotingException {
    client.insert("people", person);
}
```

The MongoDB client may be configured using these application properties.

|Name|Default|Description|
|:---|:---|:---|
|`url`||The URL of the MongoDB server|
|`username`||The user name required by the MongoDB server|
|`password`||The password required by the MongoDB server|
|`database`||The database in which the collections are stored|
|`minimum-connections`|0|The minimum number of connections to open to the MongoDB server|
|`maximum-connections`|1000|The maximum number of connections to open to the MongoDB server|
|`connection-timeout`|30 s|The amount of time to allow for a new connection to open to the MongoDB server|
|`request-timeout`|60 s|The amount of time to allow for a request to the MongoDB server to complete|
|`inactivity-timeout`|300 s|The amount of time to allow before surplus connections to the MongoDB server are pruned|
|`maximum-lifetime`|unlimited|The maximum lifetime of a connection to the MongoDB server|

&nbsp;
## Listener
Create a bean to instantiate the MongoDB listener.  The MongoDB listener polls the registered collection and executes the registered application code when new items arrive in the collection.
```
@Bean("myListener")
@ConfigurationProperties("myListener")
public MongoListener<Person> listener() {
    return MongoListenerBuilder.create(Person.class).client(client()).build();
}
```

The MongoDB listener is auto-configured from the application properties.
```
myListener.poll-interval=5
myListener.fetch-size=10000
myListener.batch-size=100
myListener.threads=5
myListener.retry-interval=60
```

Wire in the MongoDB listener and register the collection and application code.  When a collection is registered for batch processing, then it is the responsibility of the application code to ensure that the batch either succeeds atomically or fails atomically.
```
@Autowired
@Qualifier("myListener")
private MongoListener<Person> listener;

public void foo() {
    listener.register("people")
        // process batch of people
        .batch(people -> processPeople(people))
        // process individual persons; used as fallback when batch fails
        .single(person -> processPerson(person))
        .start();
}

private void processPeople(List<Person> people) {
...
}

private void processPerson(Person person) {
...
}
```

The MongoDB listener requires items to be added to the collection as managed items.  Managed items are wrapped with a unique MongoDB ID and a state.  The state allows the MongoDB listener to manage the state of the items in the collecton and to retry the processing of the items when required.
```
public void foo(Person person) throws RemotingException {
    client.insertManaged("people", person);
}
```

The MongoDB listener may be configured using these application properties.

|Name|Default|Description|
|:---|:---|:---|
|`poll-interval`|1 s|The interval at which to poll the collection for new arrivals|
|`fetch-size`|10000|The maximum number of items to retrieve from the collection during each poll|
|`batch-size`|100|The maximum number of items to process in each batch, when using batch processing|
|`threads`|1|The maximum number of threads to use when processing the items in the collection|
|`retry-interval`|60 s|The interval at which to retry items in the collection, for which processing had previously failed|
|`refresh-interval`|5 s|The interval at which to refresh the statistics that are recorded in the logs for the collection|

&nbsp;
## Service
Use the MongoDB service harness to get a service with automatic transaction correlation, along with the standard Melior logging system and a configuration object that may be used to access the application properties anywhere and at any time in the application code, even in the constructor.
```
public class MyApplication extends MongoService

public MyApplication(ServiceContext serviceContext, MongoListener<Person> listener) throws ApplicationException {
    super(serviceContext);

    registerCollection(listener, "people")
        .single(person -> processPerson(person))
        .start();
}
```

The MongoDB service harness automatically generates a unique correlation id for each transaction that originates from the MongoDB listener, either for a batch of items or for a single item, and makes the correlation id available in the transaction context for other components to access.  For example, if the REST client is used to communicate with another service then the **X-Request-Id** HTTP header is automatically populated with the correlation id.

&nbsp;  
## References
Refer to the [**Melior Service Harness :: Core**](https://github.com/MeliorArtefacts/service-harness-core) module for detail on the Melior logging system and available utilities.
