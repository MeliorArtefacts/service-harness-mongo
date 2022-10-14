/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.service.mongo;
import org.melior.client.mongo.MongoClient;

/**
 * TODO
 * @author Melior
 * @since 2.3
 */
public class MongoListenerBuilder<T>{
    private Class<T> entityClass;

    private MongoClient mongoClient;

  /**
   * Constructor.
   * @param entityClass The entity class
   */
  private MongoListenerBuilder(
    final Class<T> entityClass){
        super();

        this.entityClass = entityClass;
  }

  /**
   * Create Mongo listener builder.
   * @param <T> The type
   * @param entityClass The entity class
   * @return The Mongo listener builder
   */
  public static <T> MongoListenerBuilder<T> create(
    final Class<T> entityClass){
        return new MongoListenerBuilder<T>(entityClass);
  }

  /**
   * Build Mongo listener.
   * @return The Mongo listener
   * @throws RuntimeException if unable to build the Mongo listener
   */
  public MongoListener<T> build(){

        if (mongoClient == null){
      throw new RuntimeException( "Mongo client must be provided.");
    }

        return new MongoListener<T>(entityClass, mongoClient);
  }

  /**
   * Set Mongo client.
   * @param mongoClient The Mongo client
   * @return The Mongo listener builder
   */
  public MongoListenerBuilder<T> client(
    final MongoClient mongoClient){
        this.mongoClient = mongoClient;

    return this;
  }

}
