/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.service.mongo;
import org.melior.context.service.ServiceContext;
import org.melior.service.core.AbstractService;
import org.melior.service.exception.ApplicationException;

/**
* TODO
* @author Melior
* @since 2.3
*/
public abstract class MongoService extends AbstractService{

  /**
   * Bootstrap service.
   * @param serviceClass The service class
   * @param args The command line arguments
   */
  public static void run(
    final Class<?> serviceClass,
    final String[] args){
        AbstractService.run(serviceClass, args, true);
  }

  /**
   * Constructor.
   * @param serviceContext The service context
   * @throws ApplicationException if an error occurs during the construction
   */
  public MongoService(
    final ServiceContext serviceContext) throws ApplicationException{
        super(serviceContext, true);
  }

  /**
   * Register collection to listen to.
   * @param collectionName The collection name
   * @throws ApplicationException if unable to register the collection
   */
  protected <T> MongoCollection<T> registerCollection(
    final MongoListener<T> mongoListener,
    final String collectionName) throws ApplicationException{
        MongoCollection<T> collection;

        collection = mongoListener.registerInterceptor(collectionName);

    return collection;
  }

}
