/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.service.mongo;

/**
 * The state of the {@code MongoListener} which listens to a MongoDB collection.
 * @author Melior
 * @since 2.3
 */
public enum ListenerState {
    ACTIVE,
    SUSPENDED
}
