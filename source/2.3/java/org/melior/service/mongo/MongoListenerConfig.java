/* __  __      _ _            
  |  \/  |    | (_)           
  | \  / | ___| |_  ___  _ __ 
  | |\/| |/ _ \ | |/ _ \| '__|
  | |  | |  __/ | | (_) | |   
  |_|  |_|\___|_|_|\___/|_|   
        Service Harness
*/
package org.melior.service.mongo;
import org.melior.client.core.ClientConfig;
import org.melior.util.number.Clamp;

/**
 * Configuration parameters for a {@code MongoListener}, with defaults.
 * @author Melior
 * @since 2.3
 */
public class MongoListenerConfig extends ClientConfig {

    private int pollInterval = 1 * 1000;

    private int fetchSize = 10000;

    private int batchSize = 100;

    private int threads = 1;

    private int retryInterval = 60 * 1000;

    private int refreshInterval = 5 * 1000;

    private int recoverInterval = 60 * 1000;

    /**
     * Constructor.
     */
    protected MongoListenerConfig() {

        super();
    }

    /**
     * Get poll interval.
     * @return The poll interval
     */
    public int getPollInterval() {
        return pollInterval;
    }

    /**
     * Set poll interval.
     * @param pollInterval The poll interval, specified in seconds
     */
    public void setPollInterval(
        final int pollInterval) {
        this.pollInterval = Clamp.clampInt(pollInterval * 1000, 0, Integer.MAX_VALUE);
    }

    /**
     * Get fetch size.
     * @return The fetch size
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * Set fetch size.
     * @param fetchSize The fetch size
     */
    public void setFetchSize(
        final int fetchSize) {
        this.fetchSize = Clamp.clampInt(fetchSize, 1, Integer.MAX_VALUE);
    }

    /**
     * Get batch size.
     * @return The batch size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Set batch size.
     * @param batchSize The batch size
     */
    public void setBatchSize(
        final int batchSize) {
        this.batchSize = Clamp.clampInt(batchSize, 1, Integer.MAX_VALUE);;
    }

    /**
     * Get threads.
     * @return The threads
     */
    public int getThreads() {
        return threads;
    }

    /**
     * Set threads.
     * @param threads The threads
     */
    public void setThreads(
        final int threads) {
        this.threads = Clamp.clampInt(threads, 1, Integer.MAX_VALUE);
    }

    /**
     * Get retry interval.
     * @return The retry interval
     */
    public int getRetryInterval() {
        return retryInterval;
    }

    /**
     * Set retry interval.
     * @param retryInterval The retry interval, specified in seconds
     */
    public void setRetryInterval(
        final int retryInterval) {
        this.retryInterval = Clamp.clampInt(retryInterval * 1000, 0, Integer.MAX_VALUE);
    }

    /**
     * Get refresh interval.
     * @return The refresh interval
     */
    public int getRefreshInterval() {
        return refreshInterval;
    }

    /**
     * Set refresh interval.
     * @param refreshInterval The refresh interval, specified in seconds
     */
    public void setRefreshInterval(
        final int refreshInterval) {
        this.refreshInterval = Clamp.clampInt(refreshInterval * 1000, 0, Integer.MAX_VALUE);
    }

    /**
     * Get recover interval.
     * @return The recover interval
     */
    public int getRecoverInterval() {
        return recoverInterval;
    }

    /**
     * Set recover interval.
     * @param recoverInterval The recover interval, specified in seconds
     */
    public void setRecoverInterval(
        final int recoverInterval) {
        this.recoverInterval = Clamp.clampInt(recoverInterval * 1000, 0, Integer.MAX_VALUE);
    }

}
