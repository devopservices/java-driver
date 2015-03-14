package com.datastax.driver.core;

import org.apache.log4j.Logger;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.Result.read_request_timeout;
import static org.scassandra.http.client.PrimingRequest.Result.unavailable;
import static org.scassandra.http.client.PrimingRequest.Result.write_request_timeout;

import com.datastax.driver.core.exceptions.*;

/**
 * Tests for {@link QueryLogger} using Scassandra.
 * Contains only tests for exceptions (client timeout, read timeout, unavailable...);
 * other tests can be found in {@link QueryLoggerTest}.
 */
public class QueryLoggerErrorsTest extends ScassandraTestsSupport {

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

    @Test(groups = "short")
    public void should_log_timed_out_queries() throws Exception {
        // given
        error.setLevel(DEBUG);
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1);
        String query = "SELECT foo FROM test where bar = ?";
        primingClient.prime(
            PrimingRequest.queryBuilder()
                .withQuery(query)
                .withFixedDelay(100)
                .build()
        );
        // when
        try {
            session.execute(query);
            fail("Should have thrown NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            // ok
        }
        // then
        String line = errorAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query error")
            .contains("127.0.0.1")
            .contains(Integer.toString(scassandra.getBinaryPort()))
            .contains(query)
            .contains(DriverException.class.getName())
            .contains("Timed out waiting for server response");
    }

    @Test(groups = "short")
    public void should_log_read_timeout_errors() throws Exception {
        // given
        error.setLevel(DEBUG);
        String query = "SELECT foo FROM test where bar = ?";
        primingClient.prime(
            PrimingRequest.queryBuilder()
                .withQuery(query)
                .withResult(read_request_timeout)
                .build()
        );
        // when
        try {
            session.execute(query);
            fail("Should have thrown ReadTimeoutException");
        } catch (ReadTimeoutException e) {
            // ok
        }
        // then
        String line = errorAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query error")
            .contains("127.0.0.1")
            .contains(Integer.toString(scassandra.getBinaryPort()))
            .contains(query)
            .contains(ReadTimeoutException.class.getName());
    }

    @Test(groups = "short")
    public void should_log_write_timeout_errors() throws Exception {
        // given
        error.setLevel(DEBUG);
        String query = "UPDATE test SET foo = 'bar' where qix = ?";
        primingClient.prime(
            PrimingRequest.queryBuilder()
                .withQuery(query)
                .withResult(write_request_timeout)
                .build()
        );
        // when
        try {
            session.execute(query);
            fail("Should have thrown ReadTimeoutException");
        } catch (WriteTimeoutException e) {
            // ok
        }
        // then
        String line = errorAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query error")
            .contains("127.0.0.1")
            .contains(Integer.toString(scassandra.getBinaryPort()))
            .contains(query)
            .contains(WriteTimeoutException.class.getName());
    }

    @Test(groups = "short")
    public void should_log_unavailable_errors() throws Exception {
        // given
        error.setLevel(DEBUG);
        String query = "SELECT foo FROM test where bar = ?";
        primingClient.prime(
            PrimingRequest.queryBuilder()
                .withQuery(query)
                .withResult(unavailable)
                .build()
        );
        // when
        try {
            session.execute(query);
            fail("Should have thrown UnavailableException");
        } catch (UnavailableException e) {
            // ok
        }
        // then
        String line = errorAppender.waitAndGet(5000);
        assertThat(line)
            .contains("Query error")
            .contains("127.0.0.1")
            .contains(Integer.toString(scassandra.getBinaryPort()))
            .contains(query)
            .contains(UnavailableException.class.getName());
    }

}