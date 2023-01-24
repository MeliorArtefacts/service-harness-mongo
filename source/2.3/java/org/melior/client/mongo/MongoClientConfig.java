/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.client.mongo;
import org.melior.client.core.ClientConfig;

/**
 * Configuration parameters for a {@code MongoClient}, with defaults.
 * @author Melior
 * @since 2.3
 */
public class MongoClientConfig extends ClientConfig {

    private String database;

    /**
     * Constructor.
     */
    protected MongoClientConfig() {

        super();
    }

    /**
     * Configure client.
     * @param clientConfig The new client configuration parameters
     * @return The client configuration parameters
     */
    public MongoClientConfig configure(
        final MongoClientConfig clientConfig) {
        super.configure(clientConfig);
        this.database = clientConfig.database;

        return this;
    }

    /**
     * Get database.
     * @return The database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Set database.
     * @param database The database
     */
    public void setDatabase(
        final String database) {
        this.database = database;
    }

}
