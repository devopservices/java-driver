package com.datastax.driver.core;

/**
 * The policy with which to decide whether a host should be considered down.
 * TODO: not sure it's worth exposing this at this point. But if we do, we
 * would need to expose ConnectionException
 */
public interface ConvictionPolicy {

    /**
     * Called when a connection error occurs on a connection to the host this policy applies to.
     *
     * @param exception the connection error that occurred.
     *
     * @return {@code true} if the host should be considered down.
     */
    public boolean addFailure(ConnectionException exception);

    /**
     * Called when the host has been detected up.
     */
    public void reset();

    /**
     * Simple factory interface to allow creating {@link ConvictionPolicy} instances.
     */
    public interface Factory {

        /**
         * Creates a new ConvictionPolicy instance for {@code host}.
         *
         * @param host the host this policy applies to
         * @return the newly created {@link ConvictionPolicy} instance.
         */
        public ConvictionPolicy create(Host host);
    }
}