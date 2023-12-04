/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.service.mongo;
import java.util.List;
import org.melior.context.service.ServiceContext;
import org.melior.context.transaction.TransactionContext;
import org.melior.logging.core.Logger;
import org.melior.logging.core.LoggerFactory;
import org.melior.service.exception.ApplicationException;
import org.melior.service.exception.ExceptionType;
import org.melior.service.work.BatchProcessor;
import org.melior.service.work.SingletonProcessor;
import org.melior.service.work.WorkManager;

/**
 * Intercepts any managed items that have been added to a MongoDB collection
 * after they have been retrieved by the {@code MongoListener} but before they
 * haven been processed, and routes them past the configured {@code WorkManager}
 * to allow the {@code WorkManager} to control the flow of the items through
 * the application.
 * <p>
 * The transaction context is populated with an automatically generated UUID
 * for the duration of processing of the items.
 * @author Melior
 * @since 2.3
 */
public class MongoRequestInterceptor<T> extends MongoCollection<T> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private BatchProcessor<T> batchProcessor;

    private SingletonProcessor<T> singletonProcessor;

    private WorkManager workManager;

    /**
     * Constructor.
     * @param listener The listener
     * @param name The name of the collection
     * @param capacity The capacity of the collection
     */
    MongoRequestInterceptor(
        final MongoListener<T> listener,
        final String name,
        final int capacity) {

        super(listener, name, capacity);

        this.workManager = ServiceContext.getWorkManager();
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
        final BatchProcessor<T> batchProcessor) {
        this.batchProcessor = batchProcessor;
        super.batch(list -> processBatch(list));

        return this;
    }

    /**
     * Set singleton processor.  New arrivals in the collection
     * will be processed individually.
     * @param singletonProcessor The singleton processor
     * @return The Mongo collection
     */
    public MongoCollection<T> single(
        final SingletonProcessor<T> singletonProcessor) {
        this.singletonProcessor = singletonProcessor;
        super.single(item -> processSingle(item));

        return this;
    }

    /**
     * Process batch of items.
     * @param items The list of items
     * @throws ApplicationException if unable to process the batch of items
     */
    protected void processBatch(
        final List<T> items) throws ApplicationException {

        boolean isException = false;
        String operation;

        operation = getOperation();

        startRequest(operation);

        try {

            batchProcessor.process(items);
        }
        catch (ApplicationException exception) {

            isException = true;

            throw exception;
        }
        catch (Throwable exception) {

            isException = true;

            throw new ApplicationException(ExceptionType.UNEXPECTED, "Failed to process batch of items: " + exception.getMessage());
        }
        finally {

            completeRequest(isException);
        }

    }

    /**
     * Process item.
     * @param item The item
     * @throws ApplicationException if unable to process the item
     */
    protected void processSingle(
        final T item) throws ApplicationException {

        boolean isException = false;
        String operation;

        operation = getOperation();

        startRequest(operation);

        try {

            singletonProcessor.process(item);
        }
        catch (ApplicationException exception) {

            isException = true;

            throw exception;
        }
        catch (Throwable exception) {

            isException = true;

            throw new ApplicationException(ExceptionType.UNEXPECTED, "Failed to process item: " + exception.getMessage());
        }
        finally {

            completeRequest(isException);
        }

    }

    /**
     * Start processing request.
     * @param operation The operation
     * @throws ApplicationException if unable to start processing the request
     */
    public final void startRequest(
        final String operation) throws ApplicationException {

        String methodName = "startRequest";
        TransactionContext transactionContext;

        transactionContext = TransactionContext.get();

        transactionContext.setOperation(operation);

        try {

            workManager.startRequest(transactionContext);
        }
        catch (ApplicationException exception) {
            logger.error(methodName, "Failed to notify work manager that request has started: ", exception.getMessage(), exception);

            throw exception;
        }
        catch (Exception exception) {
            logger.error(methodName, "Failed to notify work manager that request has started: ", exception.getMessage(), exception);

            throw new ApplicationException(ExceptionType.UNEXPECTED, exception.getMessage());
        }

    }

    /**
     * Complete processing request. 
     * @param isException true if the response is an exception, false otherwise
     */
    public final void completeRequest(
        final boolean isException) {

        String methodName = "completeRequest";
        TransactionContext transactionContext;

        transactionContext = TransactionContext.get();

        try {

            workManager.completeRequest(transactionContext, isException);
        }
        catch (Exception exception) {
            logger.error(methodName, "Failed to notify work manager that request has completed: ", exception.getMessage(), exception);
        }

    }

    /**
     * Get operation.
     * @return The operation
     */
    private String getOperation() {
        return "mongo/" + getName();
    }

}
