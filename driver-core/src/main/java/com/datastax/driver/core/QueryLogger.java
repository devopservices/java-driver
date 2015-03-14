package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.concurrent.TimeUnit.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A configurable {@link LatencyTracker} that logs queries to the console.
 */
class QueryLogger implements LatencyTracker {

    // Loggers

    static final Logger NORMAL_LOGGER = LoggerFactory.getLogger(QueryLogger.class.getName() + ".NORMAL");
    static final Logger SLOW_LOGGER   = LoggerFactory.getLogger(QueryLogger.class.getName() + ".SLOW");
    static final Logger ERROR_LOGGER  = LoggerFactory.getLogger(QueryLogger.class.getName() + ".ERROR");

    // Log message templates

    private static final String NORMAL_TEMPLATE = "Query completed normally on host %s, took %s ms: %s";
    private static final String SLOW_TEMPLATE   = "Query too slow on host %s, took %s ms: %s";
    private static final String ERROR_TEMPLATE  = "Query error on host %s after %s ms: %s";

    /**
     * Store the entire Configuration
     * to be able to react to changes in QueryOptions settings.
     */
    private final Configuration configuration;

    public QueryLogger(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void update(Host host, Statement statement, Exception exception, long latencyNanos) {
        // do not log internal queries
        if(statement == Statement.DEFAULT) {
            return;
        }
        Logger logger;
        String template;
        long latencyMs = MILLISECONDS.convert(latencyNanos, NANOSECONDS);
        if(exception == null) {
            if (latencyMs > configuration.getQueryOptions().getSlowQueryLatencyThresholdMillis()) {
                logger = SLOW_LOGGER;
                template = SLOW_TEMPLATE;
            } else {
                logger = NORMAL_LOGGER;
                template = NORMAL_TEMPLATE;
            }
        } else {
            logger = ERROR_LOGGER;
            template = ERROR_TEMPLATE;
        }
        logQuery(host, statement, exception, latencyMs, logger, template);
    }

    private void logQuery(Host host, Statement statement, Exception exception, long latencyMs, Logger logger, String template) {
        if(logger.isDebugEnabled()) {
            String message = String.format(template, host, latencyMs, statementAsString(statement));
            boolean showParameterValues = statement instanceof BoundStatement && logger.isTraceEnabled();
            if (showParameterValues) {
                BoundStatement boundStatement = (BoundStatement) statement;
                ColumnDefinitions metadata = boundStatement.preparedStatement().getVariables();
                if(metadata.size() > 0) {
                    StringBuilder sb = new StringBuilder(message);
                    sb.append(" [");
                    List<ColumnDefinitions.Definition> definitions = metadata.asList();
                    for (int i = 0; i < metadata.size(); i++) {
                        if(i > 0) sb.append(", ");
                        sb.append(String.format("%s:%s", metadata.getName(i), parameterValueAsString(definitions.get(i), boundStatement.values[i])));
                    }
                    sb.append("]");
                    message = sb.toString();
                }
                logger.trace(message, exception);
            } else {
                logger.debug(message, exception);
            }
        }
    }

    private StringBuilder statementAsString(Statement statement) {
        StringBuilder sb = new StringBuilder();
        append(statement, sb, configuration.getQueryOptions().getMaxQueryStringLength());
        return sb;
    }

    private String parameterValueAsString(ColumnDefinitions.Definition definition, ByteBuffer raw) {
        String valueStr;
        if (raw == null || raw.remaining() == 0) {
            valueStr = "NULL";
        } else {
            Object value = definition.getType().deserialize(raw);
            valueStr = value.toString();
            int maxParameterValueLength = configuration.getQueryOptions().getMaxParameterValueLength();
            if (valueStr.length() > maxParameterValueLength) {
                valueStr = valueStr.substring(0, maxParameterValueLength) + "...";
            }
        }
        return valueStr;
    }

    private int append(Statement statement, StringBuilder sb, int remaining) {
        if (statement instanceof RegularStatement) {
            remaining = append(((RegularStatement)statement).getQueryString().trim(), sb, remaining);
        } else if (statement instanceof BoundStatement) {
            remaining = append(((BoundStatement)statement).preparedStatement().getQueryString().trim(), sb, remaining);
        } else if (statement instanceof BatchStatement) {
            BatchStatement batchStatement = (BatchStatement)statement;
            remaining = append("BEGIN", sb, remaining);
            switch (batchStatement.batchType) {
                case UNLOGGED:
                    append(" UNLOGGED", sb, remaining);
                    break;
                case COUNTER:
                    append(" COUNTER", sb, remaining);
                    break;
            }
            remaining = append(" BATCH", sb, remaining);
            for (Statement stmt : batchStatement.getStatements()) {
                remaining = append(" ", sb, remaining);
                remaining = append(stmt, sb, remaining);
            }
            remaining = append(" APPLY BATCH", sb, remaining);
        } else {
            // Unknown types of statement
            remaining = append("??Unknown Statement??", sb, remaining);
        }
        if(sb.charAt(sb.length() - 1) != ';') {
            remaining = append(";", sb, remaining);
        }
        return remaining;
    }

    private int append(CharSequence str, StringBuilder sb, int remaining) {
        if(remaining == -1) {
            sb.append(str);
        } else if (remaining > 0) {
            if (str.length() > remaining) {
                sb.append(str, 0, remaining).append("...");
                remaining = 0;
            } else {
                sb.append(str);
                remaining -= str.length();
            }
        }
        return remaining;
    }

}