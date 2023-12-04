/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.service.mongo;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;

/**
 * TODO
 * @author Melior
 * @since 2.3
 * @see MongoListener
 */
public class MongoSession {

    @Id
    private String id;

    private String collection;

    @Transient
    private boolean active;

    private long heartbeat;

    /**
     * Constructor.
     */
    public MongoSession() {

        super();
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
     * Get collection name.
     * @return The collection name
     */
    public String getCollection() {
        return collection;
    }

    /**
     * Set collection name.
     * @param collection The collection name
     */
    public void setCollection(
        final String collection) {
        this.collection = collection;
    }

    /**
     * Get active indicator.
     * @return The active indicator
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Set active indicator.
     * @param active The active indicator
     */
    public void setActive(
        final boolean active) {
        this.active = active;
    }

    /**
     * Get time of last heartbeat.
     * @return The time of last heartbeat
     */
    public long getHeartbeat() {
        return heartbeat;
    }

    /**
     * Set time of last heartbeat.
     * @param heartbeat The time of last heartbeat
     */
    public void setHeartbeat(
        final long heartbeat) {
        this.heartbeat = heartbeat;
    }

}
