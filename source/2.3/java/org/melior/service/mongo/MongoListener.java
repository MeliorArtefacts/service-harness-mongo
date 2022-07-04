/* __  __    _ _      
  |  \/  |  | (_)       
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
import org.melior.client.exception.RemotingException;
import org.melior.client.mongo.MongoClient;
import org.melior.client.mongo.MongoItem;
import org.melior.client.mongo.MongoState;
import org.melior.logging.core.Logger;
import org.melior.logging.core.LoggerFactory;
import org.melior.service.core.ServiceState;
import org.melior.service.exception.ExceptionType;
import org.melior.util.collection.BoundedBlockingQueue;
import org.melior.util.number.Clamp;
import org.melior.util.thread.DaemonThread;
import org.melior.util.thread.ThreadControl;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * TODO
 * @author Melior
 * @since 2.3
 */
public class MongoListener<T> extends MongoListenerConfig{
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
    final MongoClient mongoClient){
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
    final String collectionName){
        MongoCollection<T> collection;

        collection = collectionMap.get(collectionName);

        if (collection == null){
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
    final String collectionName){
        MongoCollection<T> collection;

        collection = collectionMap.get(collectionName);

        if (collection == null){
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
    final MongoCollection<T> collection){
        final MongoCollection<T> c = collection;
    DaemonThread.create(() -> listen(c));

        for (int i = 0; i < getThreads(); i++){
      DaemonThread.create(() -> process(c));
    }

        DaemonThread.create(() -> refresh(c));

        DaemonThread.create(() -> retry(c));
  }

  /**
   * Listen to collection and process new arrivals.
   * @param collection The collection
   */
  private void listen(
    final MongoCollection<T> collection){
        String methodName = "listen";
    MongoState state = MongoState.ITEM_STATE_BUSY;
    @SuppressWarnings("unchecked")
    Class<MongoItem<T>> managedEntityClass = (Class<MongoItem<T>>) new MongoItem<T>(null, null).getClass();
    List<MongoItem<T>> mongoItems;

    logger.debug(methodName, "Started listening to collection [", collection.getName(), "].");

        while (ServiceState.isActive() == true){

            while (ServiceState.isSuspended() == true){
                ThreadControl.wait(collection, 100);
      }

            while (true){
        logger.debug(methodName, "Collection [", collection.getName(), "]: total=", collection.getTotalItems().get(),
          ", failed=", collection.getFailedItems().get(), ", pending=", collection.getPendingItems().get());

        try{

                    if (state == MongoState.ITEM_STATE_BUSY){
            logger.debug(methodName, "Mark busy items as new in collection [", collection.getName(), "].");

                        mongoClient.update(collection.getName(),
              Query.query(Criteria.where("state").is(MongoState.ITEM_STATE_BUSY.getId())),
              Update.update("state", MongoState.ITEM_STATE_NEW.getId()));

                        state = MongoState.ITEM_STATE_NEW;
          }

          logger.debug(methodName, "Find new items in collection [", collection.getName(), "].");

                    mongoItems = mongoClient.find(collection.getName(), Query.query(
            Criteria.where("state").is(state.getId())).limit(getFetchSize()), managedEntityClass);

                    if (mongoItems.size() == 0){
            break;
          }

                    updateState(collection, mongoItems, MongoState.ITEM_STATE_BUSY.getId());

                    if (collection.getBatchProcessor() != null){
                        processBatches(collection, mongoItems);
          }
                    else if (collection.getSingletonProcessor() != null){
                        processSingles(collection, mongoItems);
          }

        }
        catch (Throwable exception){
          logger.error(methodName, exception.getMessage(), exception);

          break;
        }

      }

            ThreadControl.wait(collection, getPollInterval());
    }

  }

  /**
   * Process items in collection's queue.
   * @param collection The collection
   */
  private void process(
    final MongoCollection<T> collection){
        String methodName = "process";

        while (ServiceState.isActive() == true){

            while (ServiceState.isSuspended() == true){
                ThreadControl.wait(collection, 100);
      }

      try{

                if (collection.getBatchProcessor() != null){
                    processBatches(collection);
        }
                else if (collection.getSingletonProcessor() != null){
                    processSingles(collection);
        }
        else{
                    ThreadControl.wait(collection, 100);
        }

      }
      catch (Throwable exception){
        logger.error(methodName, exception.getMessage(), exception);

        break;
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
    final List<MongoItem<T>> mongoItems) throws Exception{
        BoundedBlockingQueue<List<MongoItem<T>>> queue;
    int start;
    int end;

        queue = collection.getBatchQueue();

        if (mongoItems.size() <= getBatchSize()){
            queue.add(mongoItems);
    }
    else{
            start = 0;
      end = getBatchSize();

            while (start < mongoItems.size()){
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
    final MongoCollection<T> collection) throws RemotingException{
        List<MongoItem<T>> mongoItems;

    try{
            mongoItems = collection.getBatchQueue().remove();
    }
    catch (InterruptedException exception){
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
    final List<MongoItem<T>> mongoItems) throws RemotingException{
        String methodName = "processBatch";
    List<T> items;

    try{
            items = new ArrayList<T>(mongoItems.size());

      for (MongoItem<T> mongoItem : mongoItems){
        items.add(objectMapper.convertValue(mongoItem.getItem(), entityClass));
      }

            collection.getBatchProcessor().process(items);

            delete(collection, mongoItems);

            collection.getTotalItems().increment(mongoItems.size());

            collection.getPendingItems().decrement(mongoItems.size());
    }
    catch (Throwable mutedException){

            if (collection.getSingletonProcessor() != null){
        logger.debug(methodName, "Batch processing failed.  Processing items individually.");

                for (MongoItem<T> mongoItem : mongoItems){
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
    final List<MongoItem<T>> mongoItems) throws Exception{
        BoundedBlockingQueue<MongoItem<T>> queue;

        queue = collection.getSingletonQueue();

        for (MongoItem<T> mongoItem : mongoItems){
            queue.add(mongoItem);
    }

  }

  /**
   * Process items from collection's queue.
   * @param collection The collection
   * @throws RemotingException if unable to process the items
   */
  private void processSingles(
    final MongoCollection<T> collection) throws RemotingException{
        MongoItem<T> mongoItem;

    try{
            mongoItem = collection.getSingletonQueue().remove();
    }
    catch (InterruptedException exception){
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
    final MongoItem<T> mongoItem) throws RemotingException{
        T item;

        collection.getTotalItems().increment();

    try{
            item = objectMapper.convertValue(mongoItem.getItem(), entityClass);

            collection.getSingletonProcessor().process(item);

            delete(collection, mongoItem);

            collection.getPendingItems().decrement();
    }
    catch (Throwable exception){
            collection.getFailedItems().increment();

            updateState(collection, mongoItem, MongoState.ITEM_STATE_ERROR.getId(), exception.getMessage());

            collection.getPendingItems().decrement();
    }

  }

  /**
   * Refresh collection.  Updates the number of pending items in the collection.
   * @param collection The collection
   */
  private void refresh(
    final MongoCollection<T> collection){
        String methodName = "refresh";
    long pending;

        while (ServiceState.isActive() == true){

            while (ServiceState.isSuspended() == true){
                ThreadControl.wait(collection, 100);
      }

      try{
        logger.debug(methodName, "Count number of pending items in collection [", collection.getName(), "].");

                pending = mongoClient.count(collection.getName(), Query.query(
          Criteria.where("").orOperator(
          Criteria.where("state").is(MongoState.ITEM_STATE_NEW.getId()),
          Criteria.where("state").is(MongoState.ITEM_STATE_BUSY.getId()))));

                collection.getPendingItems().reset(pending);
      }
      catch (Throwable exception){
        logger.error(methodName, exception.getMessage(), exception);

        break;
      }

            ThreadControl.wait(collection, getRefreshInterval());
    }

  }

  /**
   * Schedule items in collection for retry.
   * @param collection The collection
   */
  private void retry(
    final MongoCollection<T> collection){
        String methodName = "retry";

        while (ServiceState.isActive() == true){

            while (ServiceState.isSuspended() == true){
                ThreadControl.wait(collection, 100);
      }

      try{
        logger.debug(methodName, "Mark items with exceptions as new in collection [", collection.getName(), "].");

                mongoClient.update(collection.getName(),
          Query.query(Criteria.where("state").is(MongoState.ITEM_STATE_ERROR.getId())),
          Update.update("state", MongoState.ITEM_STATE_NEW.getId()));
      }
      catch (Throwable exception){
        logger.error(methodName, exception.getMessage(), exception);

        break;
      }

            ThreadControl.wait(collection, getRetryInterval());
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
    final String stateMessage) throws RemotingException{
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
    final String state) throws RemotingException{
        List<String> ids;

        ids = new ArrayList<String>(mongoItems.size());

    for (MongoItem mongoItem : mongoItems){
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
    final MongoItem mongoItem) throws RemotingException{
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
    final List<? extends MongoItem> mongoItems) throws RemotingException{
        List<String> ids;

        ids = new ArrayList<String>(mongoItems.size());

    for (MongoItem mongoItem : mongoItems){
      ids.add(mongoItem.getId());
    }

        mongoClient.delete(collection.getName(), Query.query(Criteria.where("_id").in(ids)));
  }

}
