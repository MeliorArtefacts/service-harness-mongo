/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.client.mongo;

/**
 * The state of a managed item which is stored in a MongoDB collection.
 * @author Melior
 * @since 2.3
 */
public enum MongoState {
    ITEM_STATE_NEW("N"),
    ITEM_STATE_BUSY("B"),
    ITEM_STATE_ERROR("E");

    private String id;

    /**
     * Constructor.
     * @param id The identifier
     */
    MongoState(
        final String id) {

        this.id = id;
    }

    /**
     * Get identifier.
     * @return The identifier
     */
    public String getId() {
        return id;
    }

}
