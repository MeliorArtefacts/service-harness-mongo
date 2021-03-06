/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.service.mongo;
import java.util.List;

import org.melior.client.mongo.MongoItem;
import org.melior.service.work.BatchProcessor;
import org.melior.service.work.SingletonProcessor;
import org.melior.util.collection.BoundedBlockingQueue;
import org.melior.util.number.ClampedCounter;
import org.melior.util.number.Counter;

/**
 * TODO
 * @author Melior
 * @since 2.3
 */
public class MongoCollection<T>{
    private MongoListener<T> listener;

    private String name;

    private BatchProcessor<T> batchProcessor;

    private SingletonProcessor<T> singletonProcessor;

    private BoundedBlockingQueue<List<MongoItem<T>>> batchQueue;

    private BoundedBlockingQueue<MongoItem<T>> singletonQueue;

    private Counter totalItems;

    private Counter failedItems;

    private ClampedCounter pendingItems;

  /**
   * Constructor.
   * @param listener The listener
   * @param name The name of the collection
   * @param capacity The capacity of the collection
   */
  public MongoCollection(
    final MongoListener<T> listener,
    final String name,
    final int capacity){
        super();

        this.listener = listener;

        this.name = name;

        batchQueue = new BoundedBlockingQueue<List<MongoItem<T>>>(capacity);

        singletonQueue = new BoundedBlockingQueue<MongoItem<T>>(capacity);

        totalItems = Counter.of(0);
    failedItems = Counter.of(0);
    pendingItems = ClampedCounter.of(0, 0, Long.MAX_VALUE);
  }

  /**
   * Set batch processor.  New arrivals in the collection will be
   * batched and processed together.  It is the responsibility of
   * the calling application to ensure that the batch either
   * succeeds atomically or fails atomically.
   * @param batchProcessor The batch processor
   * @return The Mongo collection
   */
  public MongoCollection<T> batch(
    final BatchProcessor<T> batchProcessor){
    this.batchProcessor = batchProcessor;

    return this;
  }

  /**
   * Set singleton processor.  New arrivals in the collection
   * will be processed individually.
   * @param singletonProcessor The singleton processor
   * @return The Mongo collection
   */
  public MongoCollection<T> single(
    final SingletonProcessor<T> singletonProcessor){
    this.singletonProcessor = singletonProcessor;

    return this;
  }

  /**
   * Start listening to collection.
   */
  public void start(){
    listener.start(this);
  }

  /**
   * Get name.
   * @return The name
   */
  String getName(){
    return name;
  }

  /**
   * Get batch processor.
   * @return The batch processor
   */
  BatchProcessor<T> getBatchProcessor(){
    return batchProcessor;
  }

  /**
   * Get singleton processor.
   * @return The singleton processor
   */
  SingletonProcessor<T> getSingletonProcessor(){
    return singletonProcessor;
  }

  /**
   * Get batch queue.
   * @return The batch queue
   */
  public BoundedBlockingQueue<List<MongoItem<T>>> getBatchQueue(){
    return batchQueue;
  }

  /**
   * Get singleton queue.
   * @return The singleton queue
   */
  public BoundedBlockingQueue<MongoItem<T>> getSingletonQueue(){
    return singletonQueue;
  }

  /**
   * Get total number of items.
   * @return The total number of items
   */
  public Counter getTotalItems(){
    return totalItems;
  }

  /**
   * Get number of failed items.
   * @return The number of failed items
   */
  public Counter getFailedItems(){
    return failedItems;
  }

  /**
   * Get number of pending items.
   * @return The number of pending items
   */
  public ClampedCounter getPendingItems(){
    return pendingItems;
  }

}
