/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.UnsupportedFeatureException;

/**
 * Options related to defaults for individual queries.
 */
public class QueryOptions {

    /**
     * The default consistency level for queries: {@code ConsistencyLevel.ONE}.
     */
    public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.ONE;

    /**
     * The default serial consistency level for conditional updates: {@code ConsistencyLevel.SERIAL}.
     */
    public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY_LEVEL = ConsistencyLevel.SERIAL;

    /**
     * The default fetch size for SELECT queries: 5000.
     */
    public static final int DEFAULT_FETCH_SIZE = 5000;

    /**
     * The default threshold in milliseconds beyond which queries are considered 'slow'
     * and logged as such by the driver.
     */
    public static final long DEFAULT_SLOW_QUERY_THRESHOLD_MS = 5000;

    /**
     * The default maximum length of a CQL query string that can be logged verbatim
     * by the driver. Query strings longer than this value will be truncated
     * when logged.
     */
    public static final int DEFAULT_MAX_QUERY_STRING_LENGTH = 500;

    /**
     * The default maximum length of a query parameter value that can be logged verbatim
     * by the driver. Parameter values longer than this value will be truncated
     * when logged.
     */
    public static final int DEFAULT_MAX_PARAMETER_VALUE_LENGTH = 50;


    private volatile ConsistencyLevel consistency = DEFAULT_CONSISTENCY_LEVEL;
    private volatile ConsistencyLevel serialConsistency = DEFAULT_SERIAL_CONSISTENCY_LEVEL;
    private volatile int fetchSize = DEFAULT_FETCH_SIZE;
    private volatile long slowQueryLatencyThresholdMillis = DEFAULT_SLOW_QUERY_THRESHOLD_MS;
    private volatile int maxQueryStringLength = DEFAULT_MAX_QUERY_STRING_LENGTH;
    private volatile int maxParameterValueLength = DEFAULT_MAX_PARAMETER_VALUE_LENGTH;

    private volatile Cluster.Manager manager;

    /**
     * Creates a new {@link QueryOptions} instance using the {@link #DEFAULT_CONSISTENCY_LEVEL},
     * {@link #DEFAULT_SERIAL_CONSISTENCY_LEVEL}, {@link #DEFAULT_FETCH_SIZE} and
     * {@link #DEFAULT_SLOW_QUERY_THRESHOLD_MS}.
     */
    public QueryOptions() {}

    void register(Cluster.Manager manager) {
        this.manager = manager;
    }

    /**
     * Sets the default consistency level to use for queries.
     * <p>
     * The consistency level set through this method will be use for queries
     * that don't explicitly have a consistency level, i.e. when {@link Statement#getConsistencyLevel}
     * returns {@code null}.
     *
     * @param consistencyLevel the new consistency level to set as default.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistency = consistencyLevel;
        return this;
    }

    /**
     * The default consistency level used by queries.
     *
     * @return the default consistency level used by queries.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return consistency;
    }

    /**
     * Sets the default serial consistency level to use for queries.
     * <p>
     * The serial consistency level set through this method will be use for queries
     * that don't explicitly have a serial consistency level, i.e. when {@link Statement#getSerialConsistencyLevel}
     * returns {@code null}.
     *
     * @param serialConsistencyLevel the new serial consistency level to set as default.
     * @return this {@code QueryOptions} instance.
     */
    public QueryOptions setSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel) {
        this.serialConsistency = serialConsistencyLevel;
        return this;
    }

    /**
     * The default serial consistency level used by queries.
     *
     * @return the default serial consistency level used by queries.
     */
    public ConsistencyLevel getSerialConsistencyLevel() {
        return serialConsistency;
    }

    /**
     * Sets the default fetch size to use for SELECT queries.
     * <p>
     * The fetch size set through this method will be use for queries
     * that don't explicitly have a fetch size, i.e. when {@link Statement#getFetchSize}
     * is less or equal to 0.
     *
     * @param fetchSize the new fetch size to set as default. It must be
     * strictly positive but you can use {@code Integer.MAX_VALUE} to disable
     * paging.
     * @return this {@code QueryOptions} instance.
     *
     * @throws IllegalArgumentException if {@code fetchSize &lte; 0}.
     * @throws UnsupportedFeatureException if version 1 of the native protocol is in
     * use and {@code fetchSize != Integer.MAX_VALUE} as paging is not supported by
     * version 1 of the protocol. See {@link Cluster.Builder#withProtocolVersion}
     * for more details on protocol versions.
     */
    public QueryOptions setFetchSize(int fetchSize) {
        if (fetchSize <= 0)
            throw new IllegalArgumentException("Invalid fetchSize, should be > 0, got " + fetchSize);

        int version = manager == null ? -1 : manager.protocolVersion();
        if (fetchSize != Integer.MAX_VALUE && version == 1)
            throw new UnsupportedFeatureException("Paging is not supported");

        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * The default fetch size used by queries.
     *
     * @return the default fetch size used by queries.
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * Set the threshold in milliseconds beyond which queries are considered 'slow'
     * and logged as such by the driver.
     *
     * @param slowQueryLatencyThresholdMillis Slow queries threshold in milliseconds. It must be
     * positive or zero.
     * @return this {@link QueryOptions} instance.
     * @throws IllegalArgumentException if {@code slowQueryLatencyThresholdMillis < 0}.
     */
    public QueryOptions setSlowQueryLatencyThresholdMillis(long slowQueryLatencyThresholdMillis) {
        if (slowQueryLatencyThresholdMillis < 0)
            throw new IllegalArgumentException("Invalid slowQueryLatencyThresholdMillis, should be >= 0, got " + slowQueryLatencyThresholdMillis);
        this.slowQueryLatencyThresholdMillis = slowQueryLatencyThresholdMillis;
        return this;
    }

    /**
     * The threshold in milliseconds beyond which queries are considered 'slow'
     * and logged as such by the driver.
     *
     * @return the threshold in milliseconds beyond which queries are considered 'slow'
     * and logged as such by the driver.
     */
    public long getSlowQueryLatencyThresholdMillis() {
        return slowQueryLatencyThresholdMillis;
    }

    /**
     * Set the maximum length of a CQL query string that can be logged verbatim
     * by the driver. Query strings longer than this value will be truncated
     * when logged.
     *
     * @param maxQueryStringLength The maximum length of a CQL query string
     *                             that can be logged verbatim by the driver.
     *                             It must be strictly positive or {@code -1},
     *                             in which case the query is never truncated
     *                             (use with care).
     * @return this {@link QueryOptions} instance.
     * @throws IllegalArgumentException if {@code maxQueryStringLength <= 0 && maxQueryStringLength != -1}.
     */
    public QueryOptions setMaxQueryStringLength(int maxQueryStringLength) {
        if (maxQueryStringLength <= 0 && maxQueryStringLength != -1)
            throw new IllegalArgumentException("Invalid maxQueryStringLength, should be > 0 or -1, got " + maxQueryStringLength);
        this.maxQueryStringLength = maxQueryStringLength;
        return this;
    }

    /**
     * The maximum length of a CQL query string that can be logged verbatim
     * by the driver. Query strings longer than this value will be truncated
     * when logged.
     *
     * @return The maximum length of a CQL query string that can be logged verbatim
     * by the driver.
     */
    public int getMaxQueryStringLength() {
        return maxQueryStringLength;
    }

    /**
     * Set the maximum length of a query parameter value that can be logged verbatim
     * by the driver. Parameter values longer than this value will be truncated
     * when logged.
     *
     * @param maxParameterValueLength The maximum length of a query parameter value
     *                                that can be logged verbatim by the driver.
     *                                It must be strictly positive or {@code -1},
     *                                in which case the parameter value is never truncated
     *                                (use with care).
     * @return this {@link QueryOptions} instance.
     * @throws IllegalArgumentException if {@code maxParameterValueLength <= 0 && maxParameterValueLength != -1}.
     */
    public QueryOptions setMaxParameterValueLength(int maxParameterValueLength) {
        if (maxParameterValueLength <= 0 && maxParameterValueLength != -1)
            throw new IllegalArgumentException("Invalid maxParameterValueLength, should be > 0 or -1, got " + maxParameterValueLength);
        this.maxParameterValueLength = maxParameterValueLength;
        return this;
    }

    /**
     * The maximum length of a query parameter value that can be logged verbatim
     * by the driver. Parameter values longer than this value will be truncated
     * when logged.
     *
     * @return The maximum length of a query parameter value that can be logged verbatim
     * by the driver.
     */
    public int getMaxParameterValueLength() {
        return maxParameterValueLength;
    }
}
