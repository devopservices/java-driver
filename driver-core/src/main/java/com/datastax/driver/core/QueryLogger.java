package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import static java.util.concurrent.TimeUnit.*;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A LatencyTracker that logs queries to the console.
 */
class QueryLogger implements LatencyTracker {
    
    private final long slowQueryLatencyThresholdMs;
    
    private final boolean logQueryParameters;

    private static final Logger logger = LoggerFactory.getLogger(QueryLogger.class);

    private static final Logger NORMAL_QUERIES_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.NORMAL_QUERIES");

    private static final Logger SLOW_QUERIES_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.SLOW_QUERIES");

    private static final Logger TIMEOUT_QUERIES_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.TIMEOUT_QUERIES");

    private static final Logger EXCEPTION_QUERIES_LOGGER = LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.EXCEPTION_QUERIES");

    private static final int MAX_QUERY_STRING_LENGTH = 500;

    private static final int MAX_PARAMETER_VALUE_STRING_LENGTH = 50;

    public QueryLogger(long slowQueryLatencyThresholdMs, boolean logQueryParameters) {
        this.slowQueryLatencyThresholdMs = slowQueryLatencyThresholdMs;
        this.logQueryParameters = logQueryParameters;
    }

    @Override
    public void update(Host host, Statement statement, long latencyNanos, QueryResult result) {
        try {
            // do not log internal queries
            if(statement == Statement.DEFAULT) {
                return;
            }
            long latencyMs = MILLISECONDS.convert(latencyNanos, NANOSECONDS);
            QueryLatencyLogger logger = null;
            switch (result) {
                case SUCCESS:
                    if (latencyMs > slowQueryLatencyThresholdMs) {
                        logger = new SlowQueryLatencyLogger(host, statement, latencyMs);
                    } else {
                        logger = new NormalQueryLatencyLogger(host, statement, latencyMs);
                    }
                    break;
                case TIMEOUT:
                    logger = new TimeoutQueryLatencyLogger(host, statement, latencyMs);
                    break;
                case EXCEPTION:
                    logger = new ExceptionQueryLatencyLogger(host, statement, latencyMs);
                    break;
            }
            if (logQueryParameters) {
                logger = new ParameterValuesLogger(logger, statement);
            }
            logger.log();
        } catch(RuntimeException e) {
            // Do not propagate runtime exceptions
            logger.error("Unexpected error while logging query", e);
        }
    }

    private static abstract class QueryLatencyLogger {

        public void log() {
            if(isEnabled()) {
                StringBuilder msg = buildLogMessage();
                List<Object> params = buildLogMessageParameters();
                doLog(msg.toString(), params.toArray());
            }
        }

        protected abstract boolean isEnabled();

        protected abstract StringBuilder buildLogMessage();

        protected abstract List<Object> buildLogMessageParameters();

        protected abstract void doLog(String msg, Object[] params);

    }

    private static abstract class BaseQueryLatencyLogger extends QueryLatencyLogger {

        protected final Host host;

        protected final Statement statement;

        protected final long latencyMs;

        public BaseQueryLatencyLogger(Host host, Statement statement, long latencyMs) {
            this.host = host;
            this.statement = statement;
            this.latencyMs = latencyMs;
        }

        @Override
        protected List<Object> buildLogMessageParameters() {
            QueryStringBuilder builder = new QueryStringBuilder(MAX_QUERY_STRING_LENGTH);
            builder.append(statement);
            String query = builder.toString();
            return Lists.newArrayList(host, latencyMs, query);
        }

    }

    private static class NormalQueryLatencyLogger extends BaseQueryLatencyLogger {

        private NormalQueryLatencyLogger(Host host, Statement statement, long latencyMs) {
            super(host, statement, latencyMs);
        }

        @Override
        protected boolean isEnabled() {
            return NORMAL_QUERIES_LOGGER.isDebugEnabled();
        }

        @Override
        protected StringBuilder buildLogMessage() {
            return new StringBuilder("Query completed normally on host {} took {} ms: '{}'");
        }

        @Override
        protected void doLog(String msg, Object[] params) {
            NORMAL_QUERIES_LOGGER.debug(msg, params);
        }

    }

    private static class SlowQueryLatencyLogger extends BaseQueryLatencyLogger {

        private SlowQueryLatencyLogger(Host host, Statement statement, long latencyMs) {
            super(host, statement, latencyMs);
        }
        @Override
        protected boolean isEnabled() {
            return SLOW_QUERIES_LOGGER.isWarnEnabled();
        }

        @Override
        protected StringBuilder buildLogMessage() {
            return new StringBuilder("Query too slow on host {} took {} ms: '{}'");
        }

        @Override
        protected void doLog(String msg, Object[] params) {
            SLOW_QUERIES_LOGGER.warn(msg, params);
        }

    }

    private static class TimeoutQueryLatencyLogger extends BaseQueryLatencyLogger {

        private TimeoutQueryLatencyLogger(Host host, Statement statement, long latencyMs) {
            super(host, statement, latencyMs);
        }

        @Override
        protected boolean isEnabled() {
            return TIMEOUT_QUERIES_LOGGER.isErrorEnabled();
        }

        @Override
        protected StringBuilder buildLogMessage() {
            return new StringBuilder("Query timed out on host {} after {} ms: '{}'");
        }

        @Override
        protected void doLog(String msg, Object[] params) {
            TIMEOUT_QUERIES_LOGGER.error(msg, params);
        }

    }

    private static class ExceptionQueryLatencyLogger extends BaseQueryLatencyLogger {

        private ExceptionQueryLatencyLogger(Host host, Statement statement, long latencyMs) {
            super(host, statement, latencyMs);
        }

        @Override
        protected boolean isEnabled() {
            return EXCEPTION_QUERIES_LOGGER.isErrorEnabled();
        }

        @Override
        protected StringBuilder buildLogMessage() {
            return new StringBuilder("Query threw exception on host {} after {} ms: '{}'");
        }

        @Override
        protected void doLog(String msg, Object[] params) {
            EXCEPTION_QUERIES_LOGGER.error(msg, params);
        }

    }
    private static class ParameterValuesLogger extends QueryLatencyLogger {

        private final QueryLatencyLogger decorated;

        private final Statement statement;

        private ParameterValuesLogger(QueryLatencyLogger decorated, Statement statement) {
            this.decorated = decorated;
            this.statement = statement;
        }

        @Override
        protected boolean isEnabled() {
            return decorated.isEnabled();
        }

        @Override
        protected StringBuilder buildLogMessage() {
            StringBuilder message = decorated.buildLogMessage();
            if (statement instanceof BoundStatement) {
                ColumnDefinitions metadata = ((BoundStatement)statement).preparedStatement().getVariables();
                message.append(" (actual parameters: ");
                for (int i = 0; i < metadata.size(); i++) {
                    if (i > 0)
                        message.append(", ");
                    message.append("{} = {}");
                }
                message.append(")");
            }
            return message;
        }

        @Override
        protected List<Object> buildLogMessageParameters() {
            List<Object> params = decorated.buildLogMessageParameters();
            if (statement instanceof BoundStatement) {
                ColumnDefinitions metadata = ((BoundStatement)statement).preparedStatement().getVariables();
                List<ColumnDefinitions.Definition> definitions = metadata.asList();
                for (int i = 0; i < definitions.size(); i++) {
                    ColumnDefinitions.Definition variable = definitions.get(i);
                    String name = metadata.getName(i);
                    params.add(name);
                    ByteBuffer raw = ((BoundStatement) statement).values[i];
                    String valueStr;
                    if(raw == null || raw.remaining() == 0) {
                        valueStr = "NULL";
                    } else {
                        Object value = variable.getType().deserialize(raw);
                        valueStr = value.toString();
                        if(valueStr.length() > MAX_PARAMETER_VALUE_STRING_LENGTH) {
                            valueStr = valueStr.substring(0, MAX_PARAMETER_VALUE_STRING_LENGTH) + "...";
                        }
                    }
                    params.add(valueStr);
                }
            }
            return params;
        }

        @Override
        protected void doLog(String msg, Object[] params) {
            decorated.doLog(msg, params);
        }

    }

    private static class QueryStringBuilder {

        private final StringBuilder sb;

        private int remaining;

        private QueryStringBuilder(int threshold) {
            this.remaining = threshold;
            this.sb = new StringBuilder();
        }

        public void append(Statement statement) {
            if (statement instanceof RegularStatement) {
                append(((RegularStatement)statement).getQueryString().trim());
            } else if (statement instanceof BoundStatement) {
                append(((BoundStatement)statement).preparedStatement().getQueryString().trim());
            } else if (statement instanceof BatchStatement) {
                BatchStatement batchStatement = (BatchStatement)statement;
                append("BEGIN ");
                switch (batchStatement.batchType) {
                    case UNLOGGED:
                        append("UNLOGGED");
                        break;
                    case COUNTER:
                        append("COUNTER");
                        break;
                }
                append("BATCH ");
                Collection<Statement> statements = batchStatement.getStatements();
                boolean first = true;
                for (Statement stmt : statements) {
                    if(first) {
                        first = false;
                    } else {
                        append(" ");
                    }
                    append(stmt);
                }
                append("APPLY BATCH");
            }
            if(sb.charAt(sb.length() - 1) != ';') {
                append(";");
            }
        }

        public String toString() {
            return sb.toString();
        }

        private void append(String str) {
            if (remaining > 0) {
                if (str.length() > remaining) {
                    str = str.substring(0, remaining) + "...";
                }
                sb.append(str);
                remaining -= str.length();
            }
        }

    }

}