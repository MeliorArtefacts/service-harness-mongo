/* __  __    _ _      
  |  \/  |  | (_)       
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
    Service Harness
*/
package org.melior.client.mongo;
import javax.net.ssl.SSLContext;

/**
 * TODO
 * @author Melior
 * @since 2.3
 */
public class MongoClientBuilder{
    private boolean ssl = false;

    private SSLContext sslContext;

  /**
   * Constructor.
   */
  private MongoClientBuilder(){
        super();
  }

  /**
   * Create Mongo client builder.
   * @return The Mongo client builder
   */
  public static MongoClientBuilder create(){
        return new MongoClientBuilder();
  }

  /**
   * Build Mongo client.
   * @return The Mongo client
   */
  public MongoClient build(){
        return new MongoClient(ssl, sslContext);
  }

  /**
   * Enable SSL.
   * @return The Mongo client builder
   */
  public MongoClientBuilder ssl(){
        this.ssl = true;

    return this;
  }

  /**
   * Set SSL context.
   * @param sslContext The SSL context
   * @return The Mongo client builder
   */
  public MongoClientBuilder sslContext(
    final SSLContext sslContext){
        this.sslContext = sslContext;

    return this;
  }

}
