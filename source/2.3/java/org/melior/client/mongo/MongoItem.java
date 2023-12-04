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
import org.melior.context.transaction.TransactionContext;
import org.melior.service.work.ManagedItem;
import org.springframework.data.annotation.Id;

/**
 * A managed item which is stored in a MongoDB collection. The item
 * has a unique MongoDB identifier which is automatically assigned
 * when the item is added to the collection.
 * <p>
 * A managed item is expected to be short-lived and will progress
 * through various states during its lifecycle in the collection.
 * @author Melior
 * @since 2.3
 */
public class MongoItem<T> extends ManagedItem<T> {

    @Id
    private String id;

    private String session;

    private String transaction;

    private String correlation;

    private Long eligible;

    /**
     * Constructor.
     */
    public MongoItem() {

        super(null);
    }

    /**
     * Constructor.
     * @param item The item
     * @param state The item state
     */
    public MongoItem(
        final T item,
        final String state) {

        super(item);

        setState(state);
    }

    /**
     * Constructor.
     * @param transactionContext The transaction context
     * @param item The item
     * @param state The item state
     */
    public MongoItem(
        final TransactionContext transactionContext,
        final T item,
        final String state) {

        super(item);

        setState(state);

        transaction = transactionContext.getTransactionId();

        correlation = transactionContext.getCorrelationId();
    }

    /**
     * Constructor.
     * @param transactionContext The transaction context
     * @param item The item
     * @param state The item state
     * @param delay The delay
     */
    public MongoItem(
        final TransactionContext transactionContext,
        final T item,
        final String state,
        final Duration delay) {

        this(transactionContext, item, state);

        eligible = System.currentTimeMillis() + delay.toMillis();
    }

    /**
     * Get identifier.
     * @return The identifier
     */
    public String getId() {
        return id;
    }

    /**
     * Set identifier.
     * @param id The identifier
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Get session identifier.
     * @return The session identifier
     */
    public String getSession() {
        return session;
    }

    /**
     * Set session identifier.
     * @param session The session identifier
     */
    public void setSession(
        final String session) {
        this.session = session;
    }

    /**
     * Get transaction identifier.
     * @return The transaction identifier
     */
    public String getTransaction() {
        return transaction;
    }

    /**
     * Get correlation identifier.
     * @return The correlation identifier
     */
    public String getCorrelation() {
        return correlation;
    }

    /**
     * Get time of eligibility.
     * @return The time of eligibility
     */
    public Long getEligible() {
        return eligible;
    }

}
