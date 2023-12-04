/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.service.mongo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.melior.client.exception.RemotingException;
import org.melior.client.mongo.MongoClient;
import org.melior.client.mongo.MongoItem;
import org.melior.client.mongo.ItemState;
import org.melior.context.transaction.TransactionContext;
import org.melior.logging.core.Logger;
import org.melior.logging.core.LoggerFactory;
import org.melior.service.core.ServiceState;
import org.melior.service.exception.ExceptionType;
import org.melior.util.collection.BoundedBlockingQueue;
import org.melior.util.number.Clamp;
import org.melior.util.object.ObjectUtil;
import org.melior.util.thread.DaemonThread;
import org.melior.util.thread.ThreadControl;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Implements an easy to use, auto-configuring MongoDB listener which listens
 * to registered MongoDB collections and processes any new managed items which
 * are added to the collections.
 * <p>
 * If a collection is configured with a {@code SingletonProcessor}, then any
 * new managed items that are added to the collection will be processed by
 * the listener individually.
 * <p>
 * If a collection is configured with a {@code BatchProcessor}, then any
 * new managed items that are added to the collection will be processed by
 * the listener in batches of the configured size.
 * <p>
 * If a collection is configured with both a {@code BatchProcessor} and a
 * {@code SingletonProcessor}, then the listener will use the {@code SingletonProcessor}
 * as a fall-back when processing of a batch fails.
 * <p>
 * If a collection is configured with a {@code BatchProcessor} then the implementer
 * must ensure that processing of a batch of items either succeeds atomically
 * or fails atomically.
 * <p>
 * The listener may be configured with multiple threads to speed up processing.
 * @author Melior
 * @since 2.3
 * @see MongoCollection
 * @see MongoItem
 */
public class MongoListener<T> extends MongoListenerConfig {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Class<T> entityClass;

    private MongoClient mongoClient;

    private ObjectMapper objectMapper;

    private Map<String, MongoCollection<T>> collectionMap;

    /**
     * Constructor.
     * @param entityClass The entity class
     * @param mongoClient The Mongo client
     */
    MongoListener(
        final Class<T> entityClass,
        final MongoClient mongoClient) {

        super();

        this.entityClass = entityClass;

        this.mongoClient = mongoClient;

        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        collectionMap = new HashMap<String, MongoCollection<T>>();
    }

    /**
     * Register collection to listen to.
     * @param collectionName The collection name
     * @return The collection
     */
    public MongoCollection<T> register(
        final String collectionName) {

        MongoCollection<T> collection;

        collection = collectionMap.get(collectionName);

        if (collection == null) {

            collection = new MongoCollection<T>(this, collectionName, getThreads());

            collectionMap.put(collectionName, collection);
        }

        return collection;
    }

    /**
     * Register collection to listen to.
     * @param collectionName The collection name
     * @return The collection
     */
    MongoCollection<T> registerInterceptor(
        final String collectionName) {

        MongoCollection<T> collection;

        collection = collectionMap.get(collectionName);

        if (collection == null) {

            collection = new MongoRequestInterceptor<T>(this, collectionName, getThreads());

            collectionMap.put(collectionName, collection);
        }

        return collection;
    }

    /**
     * Start listening to collection.
     * @param collection The collection
     */
    void start(
        final MongoCollection<T> collection) {

        MongoSession session;

        session = new MongoSession();
        session.setId(getSessionId());
        session.setCollection(collection.getName());

        final MongoCollection<T> c = collection;
        DaemonThread.create(() -> listen(c, session));

        for (int i = 0; i < getThreads(); i++) {
            DaemonThread.create(() -> process(c));
        }

        DaemonThread.create(() -> refresh(c, session));

        DaemonThread.create(() -> retry(c, session));

        DaemonThread.create(() -> recover(c));
    }

    /**
     * Listen to collection and process new arrivals.
     * @param collection The collection
     * @param session The session
     */
    private void listen(
        final MongoCollection<T> collection,
        final MongoSession session) {

        String methodName = "listen";
        boolean prepared = false;
        @SuppressWarnings("unchecked")
        Class<MongoItem<T>> managedEntityClass = (Class<MongoItem<T>>) new MongoItem<T>(null, null).getClass();
        List<MongoItem<T>> mongoItems;

        logger.debug(methodName, "Started listening to collection [", collection.getName(), "].");

        while (ServiceState.isActive() == true) {

            while ((ServiceState.isSuspended() == true) || (session.isActive() == false)) {

                ThreadControl.wait(collection, 100, TimeUnit.MILLISECONDS);
            }

            while (collection.getStateSupplier().get() == ListenerState.ACTIVE) {
                logger.debug(methodName, "Collection [", collection.getName(), "]: total=", collection.getTotalItems().get(),
                    ", failed=", collection.getFailedItems().get(), ", pending=", collection.getPendingItems().get());

                try {

                    if (prepared == false) {
                        logger.debug(methodName, "Set index for collection [", collection.getName(), "].");

                        mongoClient.setIndex(collection.getName(), new Document()
                            .append("state", 1)
                            .append("session", 1)
                            .append("eligible", 1));

                        prepared = true;
                    }

                    logger.debug(methodName, "Allocate new items in collection [", collection.getName(), "].");

                    if (collection.supportsDelays() == true) {

                        mongoClient.update(collection.getName(),
                            Query.query(Criteria.where("").andOperator(
                                Criteria.where("state").is(ItemState.NEW.getId()),
                                Criteria.where("session").is(null),
                                Criteria.where("eligible").lte(System.currentTimeMillis())))
                                .limit(getFetchSize()),
                            Update.update("session", session.getId()));
                    }
                    else {

                        mongoClient.update(collection.getName(),
                            Query.query(Criteria.where("").andOperator(
                                Criteria.where("state").is(ItemState.NEW.getId()),
                                Criteria.where("session").is(null)))
                                .limit(getFetchSize()),
                            Update.update("session", session.getId()));
                    }

                    mongoItems = mongoClient.find(collection.getName(),
                        Query.query(Criteria.where("").andOperator(
                            Criteria.where("state").is(ItemState.NEW.getId()),
                            Criteria.where("session").is(session.getId())))
                            .limit(getFetchSize()), managedEntityClass);

                    if (mongoItems.size() == 0) {
                        break;
                    }

                    updateState(collection, mongoItems, ItemState.BUSY.getId());

                    if (collection.getBatchProcessor() != null) {

                        processBatches(collection, mongoItems);
                    }

                    else if (collection.getSingletonProcessor() != null) {

                        processSingles(collection, mongoItems);
                    }

                }
                catch (Throwable exception) {
                    logger.error(methodName, "Failed to process new items: ", exception.getMessage(), exception);

                    break;
                }

            }

            ThreadControl.wait(collection, getPollInterval(), TimeUnit.MILLISECONDS);
        }

    }

    /**
     * Process items in collection's queue.
     * @param collection The collection
     */
    private void process(
        final MongoCollection<T> collection) {

        String methodName = "process";

        while (ServiceState.isActive() == true) {

            while (ServiceState.isSuspended() == true) {

                ThreadControl.wait(collection, 100, TimeUnit.MILLISECONDS);
            }

            try {

                if (collection.getBatchProcessor() != null) {

                    processBatches(collection);
                }

                else if (collection.getSingletonProcessor() != null) {

                    processSingles(collection);
                }
                else {

                    ThreadControl.wait(collection, 100, TimeUnit.MILLISECONDS);
                }

            }
            catch (Throwable exception) {
                logger.error(methodName, exception.getMessage(), exception);
            }

        }

    }

    /**
     * Process items in batches.
     * @param collection The collection
     * @param mongoItems The list of managed items
     * @throws Exception if unable to process the items
     */
    private void processBatches(
        final MongoCollection<T> collection,
        final List<MongoItem<T>> mongoItems) throws Exception {

        BoundedBlockingQueue<List<MongoItem<T>>> queue;
        int start;
        int end;

        queue = collection.getBatchQueue();

        if (mongoItems.size() <= getBatchSize()) {

            queue.add(mongoItems);
        }
        else {

            start = 0;
            end = getBatchSize();

            while (start < mongoItems.size()) {

                queue.add(mongoItems.subList(start, end));

                start += getBatchSize();
                end = Clamp.clampInt(start + getBatchSize(), start, mongoItems.size());
            }

        }

    }

    /**
     * Process batches of items.
     * @param collection The collection
     * @throws RemotingException if unable to process the items
     */
    private void processBatches(
        final MongoCollection<T> collection) throws RemotingException {

        List<MongoItem<T>> mongoItems;

        try {

            mongoItems = collection.getBatchQueue().remove();
        }
        catch (InterruptedException exception) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "Thread has been interrupted.");
        }

        processBatch(collection, mongoItems);
    }

    /**
     * Process batch of items.
     * @param collection The collection
     * @param mongoItems The list of managed items
     * @throws RemotingException if unable to process the items
     */
    private void processBatch(
        final MongoCollection<T> collection,
        final List<MongoItem<T>> mongoItems) throws RemotingException {

        String methodName = "processBatch";
        List<T> items;
        TransactionContext transactionContext;

        try {

            items = new ArrayList<T>(mongoItems.size());

            for (MongoItem<T> mongoItem : mongoItems) {
                items.add(objectMapper.convertValue(mongoItem.getItem(), entityClass));
            }

            transactionContext = TransactionContext.get();
            transactionContext.startTransaction();
            transactionContext.setTransactionId(getTransactionId(null));
            transactionContext.setCorrelationId(transactionContext.getTransactionId());

            try {

                collection.getBatchProcessor().process(items);
            }
            finally {

                transactionContext.reset();
            }

            delete(collection, mongoItems);

            collection.getTotalItems().increment(mongoItems.size());

            collection.getPendingItems().decrement(mongoItems.size());
        }
        catch (Throwable mutedException) {

            if (collection.getSingletonProcessor() != null) {
                logger.debug(methodName, "Batch processing failed.  Processing items individually.");

                for (MongoItem<T> mongoItem : mongoItems) {

                    processSingle(collection, mongoItem);
                }

            }

        }

    }

    /**
     * Process items individually.
     * @param collection The collection
     * @param mongoItems The list of managed items
     * @throws Exception if unable to process the items
     */
    private void processSingles(
        final MongoCollection<T> collection,
        final List<MongoItem<T>> mongoItems) throws Exception {

        BoundedBlockingQueue<MongoItem<T>> queue;

        queue = collection.getSingletonQueue();

        for (MongoItem<T> mongoItem : mongoItems) {

            queue.add(mongoItem);
        }

    }

    /**
     * Process items from collection's queue.
     * @param collection The collection
     * @throws RemotingException if unable to process the items
     */
    private void processSingles(
        final MongoCollection<T> collection) throws RemotingException {

        MongoItem<T> mongoItem;

        try {

            mongoItem = collection.getSingletonQueue().remove();
        }
        catch (InterruptedException exception) {
            throw new RemotingException(ExceptionType.LOCAL_APPLICATION, "Thread has been interrupted.");
        }

        processSingle(collection, mongoItem);
    }

    /**
     * Process item.
     * @param collection The collection
     * @param mongoItem The managed item
     * @throws RemotingException if unable to process the item
     */
    private void processSingle(
        final MongoCollection<T> collection,
        final MongoItem<T> mongoItem) throws RemotingException {

        T item;
        TransactionContext transactionContext;

        collection.getTotalItems().increment();

        try {

            item = objectMapper.convertValue(mongoItem.getItem(), entityClass);

            transactionContext = TransactionContext.get();
            transactionContext.startTransaction();
            transactionContext.setTransactionId(getTransactionId(ObjectUtil.coalesce(mongoItem.getTransaction(), mongoItem.getCorrelation())));
            transactionContext.setCorrelationId(ObjectUtil.coalesce(mongoItem.getCorrelation(), transactionContext.getTransactionId()));

            try {

                collection.getSingletonProcessor().process(item);
            }
            finally {

                transactionContext.reset();
            }

            delete(collection, mongoItem);

            collection.getPendingItems().decrement();
        }
        catch (Throwable exception) {

            collection.getFailedItems().increment();

            updateState(collection, mongoItem, ItemState.ERROR.getId(), exception.getMessage());

            collection.getPendingItems().decrement();
        }

    }

    /**
     * Refresh collection.  Updates the heartbeat for the session and
     * updates the number of pending items in the collection.
     * @param collection The collection
     * @param session The session
     */
    private void refresh(
        final MongoCollection<T> collection,
        final MongoSession session) {

        String methodName = "refresh";
        long pending;

        while (ServiceState.isActive() == true) {

            while (ServiceState.isSuspended() == true) {

                ThreadControl.wait(collection, 100, TimeUnit.MILLISECONDS);
            }

            try {
                logger.debug(methodName, "Update heartbeat for session [", session.getId(), "] for collection [", collection.getName(), "].");

                try {

                    session.setHeartbeat(System.currentTimeMillis());
                    mongoClient.update("session", session);

                    session.setActive(true);
                }
                catch (Throwable exception) {

                    session.setActive(false);

                    throw exception;
                }

                if (collection.getStateSupplier().get() == ListenerState.ACTIVE) {
                    logger.debug(methodName, "Count number of pending items in collection [", collection.getName(), "].");

                    pending = mongoClient.count(collection.getName(), Query.query(
                        Criteria.where("").orOperator(
                        Criteria.where("state").is(ItemState.NEW.getId()),
                        Criteria.where("state").is(ItemState.BUSY.getId()))));

                    collection.getPendingItems().reset(pending);
                }

            }
            catch (Throwable exception) {
                logger.error(methodName, "Failed to refresh collection: ", exception.getMessage(), exception);
            }

            ThreadControl.wait(collection, getRefreshInterval(), TimeUnit.MILLISECONDS);
        }

    }

    /**
     * Schedule items in collection for retry.
     * @param collection The collection
     * @param session The session
     */
    private void retry(
        final MongoCollection<T> collection,
        final MongoSession session) {

        String methodName = "retry";

        while (ServiceState.isActive() == true) {

            while (ServiceState.isSuspended() == true) {

                ThreadControl.wait(collection, 100, TimeUnit.MILLISECONDS);
            }

            try {

                if (collection.getStateSupplier().get() == ListenerState.ACTIVE) {
                    logger.debug(methodName, "Mark items with exceptions as new in collection [", collection.getName(), "].");

                    mongoClient.update(collection.getName(),
                        Query.query(Criteria.where("").andOperator(
                            Criteria.where("state").is(ItemState.ERROR.getId()),
                            Criteria.where("session").is(session.getId()))),
                        Update.update("state", ItemState.NEW.getId())
                            .set("session", null));
                }

            }
            catch (Throwable exception) {
                logger.error(methodName, "Failed to schedule items for retry: ", exception.getMessage(), exception);
            }

            ThreadControl.wait(collection, getRetryInterval(), TimeUnit.MILLISECONDS);
        }

    }

    /**
     * Recover abandoned items in collection.
     * @param collection The collection
     */
    private void recover(
        final MongoCollection<T> collection) {

        String methodName = "recover";
        List<MongoSession> mongoSessions;

        while (ServiceState.isActive() == true) {

            while (ServiceState.isSuspended() == true) {

                ThreadControl.wait(collection, 100, TimeUnit.MILLISECONDS);
            }

            try {

                if (collection.getStateSupplier().get() == ListenerState.ACTIVE) {
                    logger.debug(methodName, "Find expired sessions for collection [", collection.getName(), "].");

                    mongoSessions = mongoClient.find("session",
                        Query.query(Criteria.where("").andOperator(
                            Criteria.where("collection").is(collection.getName()),
                            Criteria.where("heartbeat").lte(System.currentTimeMillis() - getInactivityTimeout()))), MongoSession.class);

                    if (mongoSessions.size() > 0) {

                        for (MongoSession mongoSession : mongoSessions) {
                            logger.debug(methodName, "Recover abandoned items from session [", mongoSession.getId(), "] in collection [", collection.getName(), "].");

                            mongoClient.update(collection.getName(),
                                Query.query(Criteria.where("session").is(mongoSession.getId())),
                                Update.update("state", ItemState.NEW.getId())
                                    .set("session", null));
                        }

                        for (MongoSession mongoSession : mongoSessions) {
                            logger.debug(methodName, "Delete session [", mongoSession.getId(), "] for collection [", collection.getName(), "].");

                            mongoClient.delete("session", mongoSession);
                        }

                    }

                }

            }
            catch (Throwable exception) {
                logger.error(methodName, "Failed to recover abandoned items: ", exception.getMessage(), exception);
            }

            ThreadControl.wait(collection, getRecoverInterval(), TimeUnit.MILLISECONDS);
        }

    }

    /**
     * Update state of item in collection.
     * @param collection The collection
     * @param mongoItem The managed item
     * @param state The item state
     * @param stateMessage The state message
     * @throws RemotingException if unable to update the state of the item
     */
    private void updateState(
        final MongoCollection<T> collection,
        final MongoItem mongoItem,
        final String state,
        final String stateMessage) throws RemotingException {

        mongoItem.setState(state);
        mongoItem.setStateMessage(stateMessage);

        mongoClient.update(collection.getName(), mongoItem);
    }

    /**
     * Update state of items in collection.
     * @param collection The collection
     * @param mongoItems The list of managed items
     * @param state The item state
     * @throws RemotingException if unable to update the state of the items
     */
    private void updateState(
        final MongoCollection<T> collection,
        final List<? extends MongoItem> mongoItems,
        final String state) throws RemotingException {

        List<String> ids;

        ids = new ArrayList<String>(mongoItems.size());

        for (MongoItem mongoItem : mongoItems) {
            ids.add(mongoItem.getId());
        }

        mongoClient.update(collection.getName(), Query.query(Criteria.where("_id").in(ids)), Update.update("state", state));
    }

    /**
     * Delete item from collection.
     * @param collection The collection
     * @param mongoItem The managed item
     * @throws RemotingException if unable to delete the item
     */
    private void delete(
        final MongoCollection<T> collection,
        final MongoItem mongoItem) throws RemotingException {

        mongoClient.delete(collection.getName(), mongoItem);
    }

    /**
     * Delete items from collection.
     * @param collection The collection
     * @param mongoItems The list of managed items
     * @throws RemotingException if unable to delete the items
     */
    private void delete(
        final MongoCollection<T> collection,
        final List<? extends MongoItem> mongoItems) throws RemotingException {

        List<String> ids;

        ids = new ArrayList<String>(mongoItems.size());

        for (MongoItem mongoItem : mongoItems) {
            ids.add(mongoItem.getId());
        }

        mongoClient.delete(collection.getName(), Query.query(Criteria.where("_id").in(ids)));
    }

    /**
     * Get session identifier.  Generates a UUID.
     * @return The resultant session identifier
     */
    private String getSessionId() {

        return UUID.randomUUID().toString();
    }

    /**
     * Get transaction identifier.  Generates a UUID if the transaction identifier is undefined.
     * @param transactionId The provided transaction identifier
     * @return The resultant transaction identifier
     */
    private String getTransactionId(
        final String transactionId) {

        return ObjectUtil.coalesce(transactionId, UUID.randomUUID().toString());
    }

}
