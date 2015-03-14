package com.datastax.driver.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.INFO;
import static org.apache.log4j.Level.TRACE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.driver.core.exceptions.DriverException;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.datastax.driver.core.CCMBridge.ipOfNode;
import static com.datastax.driver.core.QueryOptions.DEFAULT_MAX_PARAMETER_VALUE_LENGTH;
import static com.datastax.driver.core.QueryOptions.DEFAULT_MAX_QUERY_STRING_LENGTH;
import static com.datastax.driver.core.QueryOptions.DEFAULT_SLOW_QUERY_THRESHOLD_MS;
import static com.datastax.driver.core.TestUtils.getFixedValue;

/**
 * Tests for {@link QueryLogger} using CCMBdrige.
 * Tests for exceptions (client timeout, read timeout, unavailable...) can be found in
 * {@link QueryLoggerErrorsTest}.
 */
public class QueryLoggerTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final List<DataType> dataTypes = new ArrayList<DataType>(Sets.filter(DataType.allPrimitiveTypes(), new Predicate<DataType>() {
        @Override
        public boolean apply(DataType type) {
            return type != DataType.counter();
        }
    }));

    private static final List<Object> values = Lists.transform(dataTypes, new Function<DataType, Object>() {
            @Override
            public Object apply(DataType type) {
                return getFixedValue(type);
            }
        }
    );

    private static final String definitions = Joiner.on(", ").join(
        Lists.transform(dataTypes, new Function<DataType, String>() {
                @Override
                public String apply(DataType type) {
                    return "c_" + type + " " + type;
                }
            }
        )
    );

    private static final String assignments = Joiner.on(", ").join(
        Lists.transform(dataTypes, new Function<DataType, String>() {
                @Override
                public String apply(DataType type) {
                    return "c_" + type + " = ?";
                }
            }
        )
    );

    private Logger normal = Logger.getLogger(QueryLogger.NORMAL_LOGGER.getName());
    private Logger slow = Logger.getLogger(QueryLogger.SLOW_LOGGER.getName());
    private Logger error = Logger.getLogger(QueryLogger.ERROR_LOGGER.getName());

    private MemoryAppender normalAppender;
    private MemoryAppender slowAppender;
    private MemoryAppender errorAppender;

    @BeforeMethod(groups = { "short", "unit" })
    public void startCapturingLogs() {
        normal.addAppender(normalAppender = new MemoryAppender());
        slow.addAppender(slowAppender = new MemoryAppender());
        error.addAppender(errorAppender = new MemoryAppender());
    }

    @AfterMethod(groups = { "short", "unit" })
    public void stopCapturingLogs() {
        normal.setLevel(null);
        slow.setLevel(null);
        error.setLevel(null);
        normal.removeAppender(normalAppender);
        slow.removeAppender(slowAppender);
        error.removeAppender(errorAppender);
    }

    @BeforeMethod(groups = { "short", "unit" })
    @AfterMethod(groups = { "short", "unit" })
    public void resetLogLevels() {
        normal.setLevel(INFO);
        slow.setLevel(INFO);
        error.setLevel(INFO);
    }

    @BeforeMethod(groups = "short")
    public void resetQueryLoggerConfiguration() {
        QueryOptions queryOptions = cluster.getConfiguration().getQueryOptions();
        queryOptions.setSlowQueryLatencyThresholdMillis(DEFAULT_SLOW_QUERY_THRESHOLD_MS);
        queryOptions.setMaxQueryStringLength(DEFAULT_MAX_QUERY_STRING_LENGTH);
        queryOptions.setMaxParameterValueLength(DEFAULT_MAX_PARAMETER_VALUE_LENGTH);
    }

    // Tests for different types of statements (Regular, Bound, Batch)

    @Test(groups = "short")
    public void should_log_regular_statements() throws Exception {
        // given
        normal.setLevel(DEBUG);
        setSlowQueryLatencyThresholdMillis(Long.MAX_VALUE);
        // when
        String query = "SELECT c_text FROM test WHERE pk = ?";
        session.execute(query, 42);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains(query)
            .doesNotContain("parameters");
    }

    @Test(groups = "short")
    public void should_log_bound_statements() throws Exception {
        // given
        normal.setLevel(DEBUG);
        setSlowQueryLatencyThresholdMillis(Long.MAX_VALUE);
        // when
        String query = "SELECT * FROM test where pk = ?";
        PreparedStatement ps = session.prepare(query);
        BoundStatement bs = ps.bind(42);
        session.execute(bs);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains(query)
            .doesNotContain("actual parameters");
    }

    @Test(groups = "short")
    public void should_log_batch_statements() throws Exception {
        // given
        normal.setLevel(DEBUG);
        setSlowQueryLatencyThresholdMillis(Long.MAX_VALUE);
        setMaxQueryStringLength(Integer.MAX_VALUE);
        // when
        String query1 = "INSERT INTO test (pk) VALUES (?)";
        String query2 = "UPDATE test SET c_int = ? WHERE pk = 42";
        PreparedStatement ps1 = session.prepare(query1);
        PreparedStatement ps2 = session.prepare(query2);
        BatchStatement batch = new BatchStatement();
        batch.add(ps1.bind(42));
        batch.add(ps2.bind(1234));
        session.execute(batch);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains("BEGIN BATCH")
            .contains("APPLY BATCH")
            .contains(query1)
            .contains(query2)
            .doesNotContain("c_int:");
    }

    @Test(groups = "short")
    public void should_log_unlogged_batch_statements() throws Exception {
        // given
        normal.setLevel(DEBUG);
        setSlowQueryLatencyThresholdMillis(Long.MAX_VALUE);
        setMaxQueryStringLength(Integer.MAX_VALUE);
        // when
        String query1 = "INSERT INTO test (pk) VALUES (?)";
        String query2 = "UPDATE test SET c_int = ? WHERE pk = 42";
        PreparedStatement ps1 = session.prepare(query1);
        PreparedStatement ps2 = session.prepare(query2);
        BatchStatement batch = new BatchStatement(UNLOGGED);
        batch.add(ps1.bind(42));
        batch.add(ps2.bind(1234));
        session.execute(batch);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains("BEGIN UNLOGGED BATCH")
            .contains("APPLY BATCH")
            .contains(query1)
            .contains(query2)
            .doesNotContain("c_int:");
    }

    @Test(groups = "unit")
    public void should_not_log_special_statements() throws Exception {
        // given
        normal.setLevel(DEBUG);
        // when
        new QueryLogger(null).update(null, Statement.DEFAULT, null, 0);
        // then
        String line = normalAppender.get();
        assertThat(line).isEmpty();
    }

    // Tests for different log levels

    @Test(groups = "unit")
    public void should_not_log_normal_if_level_higher_than_DEBUG() throws Exception {
        // given
        normal.setLevel(INFO);
        slow.setLevel(INFO);
        error.setLevel(INFO);
        // when
        new QueryLogger(new Configuration()).update(null, mock(BoundStatement.class), null, 0);
        // then
        assertThat(normalAppender.get()).isEmpty();
        assertThat(slowAppender.get()).isEmpty();
        assertThat(errorAppender.get()).isEmpty();
    }

    @Test(groups = "unit")
    public void should_not_log_slow_if_level_higher_than_DEBUG() throws Exception {
        // given
        normal.setLevel(INFO);
        slow.setLevel(INFO);
        error.setLevel(INFO);
        // when
        new QueryLogger(new Configuration()).update(null, mock(BoundStatement.class), null, DEFAULT_SLOW_QUERY_THRESHOLD_MS + 1);
        // then
        assertThat(normalAppender.get()).isEmpty();
        assertThat(slowAppender.get()).isEmpty();
        assertThat(errorAppender.get()).isEmpty();
    }

    @Test(groups = "unit")
    public void should_not_log_error_if_level_higher_than_DEBUG() throws Exception {
        // given
        normal.setLevel(INFO);
        slow.setLevel(INFO);
        error.setLevel(INFO);
        // when
        new QueryLogger(new Configuration()).update(null, mock(BoundStatement.class), new DriverException("booh"), 0);
        // then
        assertThat(normalAppender.get()).isEmpty();
        assertThat(slowAppender.get()).isEmpty();
        assertThat(errorAppender.get()).isEmpty();
    }

    // Tests for different query types (normal, slow, exception)

    @Test(groups = "short")
    public void should_log_normal_queries() throws Exception {
        // given
        normal.setLevel(DEBUG);
        setSlowQueryLatencyThresholdMillis(Long.MAX_VALUE);
        // when
        String query = "SELECT * FROM test where pk = ?";
        PreparedStatement ps = session.prepare(query);
        BoundStatement bs = ps.bind(42);
        session.execute(bs);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains(query)
            .doesNotContain("pk:42");
    }

    @Test(groups = "short")
    public void should_log_slow_queries() throws Exception {
        // given
        slow.setLevel(DEBUG);
        setSlowQueryLatencyThresholdMillis(0);
        // when
        String query = "SELECT * FROM test where pk = ?";
        PreparedStatement ps = session.prepare(query);
        BoundStatement bs = ps.bind(42);
        session.execute(bs);
        // then
        String line = slowAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query too slow")
            .contains(ipOfNode(1))
            .contains(query)
            .doesNotContain("pk:42");
    }

    // Tests with query parameters (log level TRACE)

    @Test(groups = "short")
    public void should_log_non_null_named_parameter() throws Exception {
        // given
        normal.setLevel(TRACE);
        // when
        String query = "UPDATE test SET c_text = :param1 WHERE pk = :param2";
        PreparedStatement ps = session.prepare(query);
        BoundStatement bs = ps.bind();
        bs.setString("param1", "foo");
        bs.setInt("param2", 42);
        session.execute(bs);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains(query)
            .contains("param2:42")
            .contains("param1:foo");
    }

    @Test(groups = "short")
    public void should_log_non_null_positional_parameter() throws Exception {
        // given
        normal.setLevel(TRACE);
        // when
        String query = "UPDATE test SET c_text = ? WHERE pk = ?";
        PreparedStatement ps = session.prepare(query);
        BoundStatement bs = ps.bind();
        bs.setString("c_text", "foo");
        bs.setInt("pk", 42);
        session.execute(bs);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains(query)
            .contains("pk:42")
            .contains("c_text:foo");
    }

    @Test(groups = "short")
    public void should_log_null_parameter() throws Exception {
        // given
        normal.setLevel(TRACE);
        // when
        String query = "UPDATE test SET c_text = ? WHERE pk = ?";
        PreparedStatement ps = session.prepare(query);
        BoundStatement bs = ps.bind();
        bs.setString("c_text", null);
        bs.setInt("pk", 42);
        session.execute(bs);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains(query)
            .contains("pk:42")
            .contains("c_text:NULL");
    }

    // Test different CQL types

    @Test(groups = "short")
    public void should_log_all_parameter_types() throws Exception {
        // given
        normal.setLevel(TRACE);
        setMaxParameterValueLength(Integer.MAX_VALUE);
        // when
        String query = "UPDATE test SET " + assignments + " WHERE pk = 42";
        PreparedStatement ps = session.prepare(query);
        BoundStatement bs = ps.bind(values.toArray());
        session.execute(bs);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains(query);
        for (DataType type : dataTypes) {
            assertThat(line).contains(getFixedValue(type).toString());
        }
    }

    // Tests for truncation of query strings and parameter values

    @Test(groups = "short")
    public void should_log_truncated_query() throws Exception {
        // given
        normal.setLevel(DEBUG);
        setMaxQueryStringLength(5);
        // when
        String query = "SELECT * FROM test WHERE pk = 42";
        session.execute(query);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains(query.substring(0, 5) + "...")
            .doesNotContain(query);
    }

    @Test(groups = "short")
    public void should_log_truncated_parameter() throws Exception {
        // given
        normal.setLevel(TRACE);
        setMaxParameterValueLength(5);
        // when
        String query = "UPDATE test SET c_int = ? WHERE pk = ?";
        PreparedStatement ps = session.prepare(query);
        BoundStatement bs = ps.bind();
        bs.setInt("c_int", 123456);
        bs.setInt("pk", 42);
        session.execute(bs);
        // then
        String line = normalAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query completed normally")
            .contains(ipOfNode(1))
            .contains("c_int:12345...")
            .doesNotContain("123456");
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE test (pk int PRIMARY KEY, " + definitions + ")");
    }

    private void setSlowQueryLatencyThresholdMillis(long value) {
        cluster.getConfiguration().getQueryOptions().setSlowQueryLatencyThresholdMillis(value);
    }

    private void setMaxQueryStringLength(int value) {
        cluster.getConfiguration().getQueryOptions().setMaxQueryStringLength(value);
    }

    private void setMaxParameterValueLength(int value) {
        cluster.getConfiguration().getQueryOptions().setMaxParameterValueLength(value);
    }
}