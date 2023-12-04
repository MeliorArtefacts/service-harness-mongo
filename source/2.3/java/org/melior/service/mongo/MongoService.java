/* __  __      _ _            
  |  \/  |    | (_)           
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
 * A base class for service implementations that process managed items from
 * MongoDB collections.
 * <p>
 * The service implementation class is furnished with important constructs
 * like the service context, the service configuration and a logger.
 * <p>
 * If the service implementation class registers a MongoDB collection with
 * a {@code MongoListener} then this class installs a {@code MongoRequestInterceptor}
 * to intercept the managed items that have been added to the collection
 * after they have been retrieved by the {@code MongoListener} but before they
 * haven been processed, and routes them past the configured {@code WorkManager}
 * to allow the {@code WorkManager} to control the flow of the items through
 * the application.
 * @author Melior
 * @since 2.3
 * @see MongoRequestInterceptor
 * @see AbstractService
*/
public abstract class MongoService extends AbstractService {

    /**
     * Bootstrap service.
     * @param serviceClass The service class
     * @param args The command line arguments
     */
    public static void run(
        final Class<?> serviceClass,
        final String[] args) {

        AbstractService.run(serviceClass, args, true);
    }

    /**
     * Constructor.
     * @param serviceContext The service context
     * @throws ApplicationException if an error occurs during the construction
     */
    public MongoService(
        final ServiceContext serviceContext) throws ApplicationException {

        super(serviceContext, true);
    }

    /**
     * Register collection to listen to.
     * @param <T> The type
     * @param mongoListener The Mongo listener
     * @param collectionName The collection name
     * @return The Mongo collection
     * @throws ApplicationException if unable to register the collection
     */
    protected <T> MongoCollection<T> registerCollection(
        final MongoListener<T> mongoListener,
        final String collectionName) throws ApplicationException {

        MongoCollection<T> collection;

        collection = mongoListener.registerInterceptor(collectionName);

        return collection;
    }

}
