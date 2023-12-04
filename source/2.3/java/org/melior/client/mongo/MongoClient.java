/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.client.mongo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.bson.Document;
import org.melior.client.exception.RemotingException;
import org.melior.context.transaction.TransactionContext;
import org.melior.logging.core.Logger;
import org.melior.logging.core.LoggerFactory;
import org.melior.service.exception.ExceptionType;
import org.melior.util.object.ObjectUtil;
import org.melior.util.string.StringUtil;
import org.melior.util.time.Timer;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.StringUtils;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClients;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * Implements an easy to use, auto-configuring MongoDB client with connection
 * pooling, automatic object mapping and support for storing managed items in
 * the MongoDB collections.
 * <p>
 * The client writes timing details to the logs while dispatching MongoDB messages
 * to the MongoDB server.  The client automatically converts any exception that
 * occurs during communication with the MongoDB server into a standard
 * {@code RemotingException}.
 * @author Melior
 * @since 2.3
 */
public class MongoClient extends MongoClientConfig {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private boolean ssl;

    private SSLContext sslContext;

    private MongoTemplate mongoTemplate;

    /**
     * Constructor.
     * @param ssl The SSL indicator
     * @param sslContext The SSL context
     */
    MongoClient(
        final boolean ssl,
        final SSLContext sslContext) {

        super();

        this.ssl = ssl;

        this.sslContext = sslContext;
    }

    /**
     * Configure client.
     * @param clientConfig The new client configuration parameters
     * @return The Mongo client
     */
    public MongoClient configure(
        final MongoClientConfig clientConfig) {
        super.configure(clientConfig);

        return this;
    }

    /**
     * Initialize client.
     * @throws RemotingException if unable to initialize the client
     */
    private void initialize() throws RemotingException {

        ConnectionString connectionString;
        MongoClientSettings.Builder clientSettings;
        com.mongodb.client.MongoClient client;
        MongoDatabaseFactory databaseFactory;
        MappingMongoConverter mappingConverter;

        if (mongoTemplate != null) {
            return;
        }

        if (StringUtils.hasLength(getUrl()) == false) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "URL must be configured.");
        }

        connectionString = new ConnectionString(getUrl());

        clientSettings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .applyToConnectionPoolSettings(builder -> builder
                .minSize(getMinimumConnections())
                .maxSize(getMaximumConnections())
                .maxWaitTime(getConnectionTimeout(), TimeUnit.MILLISECONDS)
                .maxConnectionIdleTime(getInactivityTimeout(), TimeUnit.MILLISECONDS)
                .maxConnectionLifeTime(getMaximumLifetime(), TimeUnit.MILLISECONDS))
            .applyToSocketSettings(builder -> builder
                .connectTimeout(getConnectionTimeout(), TimeUnit.MILLISECONDS)
                .readTimeout(getRequestTimeout(), TimeUnit.MILLISECONDS));

        if ((StringUtils.hasLength(getUsername()) == true) && (StringUtils.hasLength(getPassword()) == true)) {

            clientSettings.credential(MongoCredential.createCredential(getUsername(),
                ObjectUtil.coalesce(connectionString.getDatabase(), "admin"), getPassword().toCharArray()));
        }

        if (ssl == true) {

            clientSettings.applyToSslSettings(builder -> builder
                .enabled(true)
                .invalidHostNameAllowed(true)
                .context(sslContext));
        }

        client = MongoClients.create(clientSettings.build());

        databaseFactory = new SimpleMongoClientDatabaseFactory(client, getDatabase());

        mappingConverter = new MappingMongoConverter(new DefaultDbRefResolver(databaseFactory), new MongoMappingContext());
        mappingConverter.setTypeMapper(new DefaultMongoTypeMapper(null));

        mongoTemplate = new MongoTemplate(databaseFactory, mappingConverter);
    }

    /**
     * Set index for collection.
     * @param collectionName The collection name
     * @param indexDefinition The index definition
     * @throws RemotingException if unable to set the index
     */
    public void setIndex(
        final String collectionName,
        final Document indexDefinition) throws RemotingException {

        try {

            mongoTemplate.indexOps(collectionName).ensureIndex(new CompoundIndexDefinition(indexDefinition));
        }
        catch (Exception exception) {

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, "Failed to set index: " + exception.getMessage(), exception);
        }

    }

    /**
     * Insert item in collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param item The item
     * @throws RemotingException if unable to insert the item
     */
    public <T> void insert(
        final String collectionName,
        final T item) throws RemotingException {

        String methodName = "insert";
        Timer timer;
        long duration;

        initialize();

        logger.debug(methodName, "Insert 1 item in collection [", collectionName, "].");

        timer = Timer.ofNanos().start();

        try {

            mongoTemplate.insert(item, collectionName);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Item inserted successfully.  Duration = ", duration, " ms.");
        }
        catch (RuntimeException exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Item insert failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, exception.getMessage(), exception);
        }
        catch (Exception exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Item insert failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, "Failed to insert item: " + exception.getMessage(), exception);
        }

    }

    /**
     * Insert managed item in collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param item The item
     * @throws RemotingException if unable to insert the item
     */
    public <T> void insertManaged(
        final String collectionName,
        final T item) throws RemotingException {

        insert(collectionName, new MongoItem<T>(TransactionContext.get(), item, ItemState.NEW.getId()));
    }

    /**
     * Insert managed item in collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param item The item
     * @param delay The delay
     * @throws RemotingException if unable to insert the item
     */
    public <T> void insertManaged(
        final String collectionName,
        final T item,
        final Duration delay) throws RemotingException {

        insert(collectionName, new MongoItem<T>(TransactionContext.get(), item, ItemState.NEW.getId(), delay));
    }

    /**
     * Insert items in collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param items The list of items
     * @throws RemotingException if unable to insert the items
     */
    @SuppressWarnings("unchecked")
    public <T> void insert(
        final String collectionName,
        final T... items) throws RemotingException {

        insert(collectionName, Arrays.asList(items));
    }

    /**
     * Insert items in collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param items The list of items
     * @throws RemotingException if unable to insert the items
     */
    public <T> void insert(
        final String collectionName,
        final Collection<T> items) throws RemotingException {

        String methodName = "insert";
        Timer timer;
        long duration;

        initialize();

        logger.debug(methodName, "Insert ", items.size(), " items in collection [", collectionName, "].");

        timer = Timer.ofNanos().start();

        try {

            mongoTemplate.insert(items, collectionName);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Items inserted successfully.  Duration = ", duration, " ms.");
        }
        catch (RuntimeException exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Items insert failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, exception.getMessage(), exception);
        }
        catch (Exception exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Items insert failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, "Failed to insert items: " + exception.getMessage(), exception);
        }

    }

    /**
     * Insert managed items in collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param items The list of items
     * @throws RemotingException if unable to insert the items
     */
    public <T> void insertManaged(
        final String collectionName,
        final Collection<T> items) throws RemotingException {

        Collection<MongoItem<T>> managedItems;

        managedItems = new ArrayList<MongoItem<T>>(items.size());

        for (T item : items) {
            managedItems.add(new MongoItem<T>(TransactionContext.get(), item, ItemState.NEW.getId()));
        }

        insert(collectionName, managedItems);
    }

    /**
     * Insert managed items in collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param items The list of items
     * @param delay The delay
     * @throws RemotingException if unable to insert the items
     */
    public <T> void insertManaged(
        final String collectionName,
        final Collection<T> items,
        final Duration delay) throws RemotingException {

        Collection<MongoItem<T>> managedItems;

        managedItems = new ArrayList<MongoItem<T>>(items.size());

        for (T item : items) {
            managedItems.add(new MongoItem<T>(TransactionContext.get(), item, ItemState.NEW.getId(), delay));
        }

        insert(collectionName, managedItems);
    }

    /**
     * Update item in collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param item The item
     * @throws RemotingException if unable to update the item
     */
    public <T> void update(
        final String collectionName,
        final T item) throws RemotingException {

        String methodName = "update";
        Timer timer;
        long duration;

        initialize();

        logger.debug(methodName, "Update 1 item in collection [", collectionName, "].");

        timer = Timer.ofNanos().start();

        try {

            mongoTemplate.save(item, collectionName);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Item updated successfully.  Duration = ", duration, " ms.");
        }
        catch (RuntimeException exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Item update failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, exception.getMessage(), exception);
        }
        catch (Exception exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Item update failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, "Failed to update item: " + exception.getMessage(), exception);
        }

    }

    /**
     * Update items in collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param query The query to use
     * @param update The update to apply
     * @throws RemotingException if unable to update items
     */
    public <T> void update(
        final String collectionName,
        final Query query,
        final Update update) throws RemotingException {

        String methodName = "update";
        Timer timer;
        UpdateResult updateResult;
        long duration;

        initialize();

        logger.debug(methodName, "Update items in collection [", collectionName, "]. ", trim(update.getUpdateObject()));

        timer = Timer.ofNanos().start();

        try {

            updateResult = mongoTemplate.updateMulti(query, update, collectionName);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, updateResult.getModifiedCount(), " items updated successfully.  Duration = ", duration, " ms.");
        }
        catch (RuntimeException exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Items update failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, exception.getMessage(), exception);
        }
        catch (Exception exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Items update failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, "Failed to update items: " + exception.getMessage(), exception);
        }

    }

    /**
     * Delete item from collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param item The item
     * @throws RemotingException if unable to delete the item
     */
    public <T> void delete(
        final String collectionName,
        final T item) throws RemotingException {

        String methodName = "delete";
        Timer timer;
        long duration;

        initialize();

        logger.debug(methodName, "Delete 1 item in collection [", collectionName, "].");

        timer = Timer.ofNanos().start();

        try {

            mongoTemplate.remove(item, collectionName);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Item deleted successfully.  Duration = ", duration, " ms.");
        }
        catch (RuntimeException exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Item delete failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, exception.getMessage(), exception);
        }
        catch (Exception exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Item delete failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, "Failed to delete item: " + exception.getMessage(), exception);
        }

    }

    /**
     * Delete items from collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param query The query to use
     * @throws RemotingException if unable to delete the items
     */
    public <T> void delete(
        final String collectionName,
        final Query query) throws RemotingException {

        String methodName = "delete";
        Timer timer;
        DeleteResult deleteResult;
        long duration;

        initialize();

        logger.debug(methodName, "Delete items in collection [", collectionName, "].");

        timer = Timer.ofNanos().start();

        try {

            deleteResult = mongoTemplate.remove(query, collectionName);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, deleteResult.getDeletedCount(), " items deleted successfully.  Duration = ", duration, " ms.");
        }
        catch (RuntimeException exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Items delete failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, exception.getMessage(), exception);
        }
        catch (Exception exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Items delete failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, "Failed to delete items: " + exception.getMessage(), exception);
        }

    }

    /**
     * Find items in collection.
     * @param <T> The type
     * @param collectionName The collection name
     * @param query The query to use
     * @param entityClass The result entity class
     * @return The list of items
     * @throws RemotingException if unable to find items
     */
    public <T> List<T> find(
        final String collectionName,
        final Query query,
        final Class<T> entityClass) throws RemotingException {

        String methodName = "find";
        Timer timer;
        List<T> items;
        long duration;

        initialize();

        logger.debug(methodName, "Find items in collection [", collectionName, "]. ", trim(query.getQueryObject()));

        timer = Timer.ofNanos().start();

        try {

            items = mongoTemplate.find(query, entityClass, collectionName);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Found ", items.size(), " items.  Duration = ", duration, " ms.");
        }
        catch (RuntimeException exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Items find failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, exception.getMessage(), exception);
        }
        catch (Exception exception) {
            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, "Failed to find items: " + exception.getMessage(), exception);
        }

        return items;
    }

    /**
     * Count items in collection.
     * @param collectionName The collection name
     * @param query The query to use
     * @return The number of items found
     * @throws RemotingException if unable to count the items
     */
    public long count(
        final String collectionName,
        final Query query) throws RemotingException {

        String methodName = "count";
        Timer timer;
        long count;
        long duration;

        initialize();

        logger.debug(methodName, "Count items in collection [", collectionName, "]. ", trim(query.getQueryObject()));

        timer = Timer.ofNanos().start();

        try {

            count = mongoTemplate.count(query, collectionName);

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Counted ", count, " items.  Duration = ", duration, " ms.");
        }
        catch (RuntimeException exception) {

            duration = timer.elapsedTime(TimeUnit.MILLISECONDS);

            logger.debug(methodName, "Items count failed.  Duration = ", duration, " ms.");

            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, exception.getMessage(), exception);
        }
        catch (Exception exception) {
            throw new RemotingException(ExceptionType.REMOTING_COMMUNICATION, "Failed to count items: " + exception.getMessage(), exception);
        }

        return count;
    }

    /**
     * Trim document to more concise format.
     * @param document The document
     * @return The trimmed document
     */
    private String trim(
        final Document document) {

        StringBuilder stringBuilder;

        stringBuilder = new StringBuilder(document.toString());
        StringUtil.replaceAll(stringBuilder, "Document", "");
        StringUtil.replaceAll(stringBuilder, "{{", "{");
        StringUtil.replaceAll(stringBuilder, "}}", "}");

        return stringBuilder.toString();
    }

}
