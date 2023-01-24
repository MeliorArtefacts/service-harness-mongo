/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.client.mongo;
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

}
