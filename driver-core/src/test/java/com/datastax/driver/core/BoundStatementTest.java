package com.datastax.driver.core;

import java.util.Collection;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.exceptions.InvalidTypeException;

public class BoundStatementTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE foo (k int primary key, v text)");
    }

    @Test(groups="short")
    public void should_expose_value_getters() {
        // We don't need to be exhaustive, the getter code is shared with ArrayBackedRow which is extensively covered in DataTypeTest.
        BoundStatement statement = session.prepare("INSERT INTO foo (k, v) VALUES (?, ?)").bind();

        statement.bind(1, "test");

        assertThat(statement.getInt(0))
            .isEqualTo(statement.getInt("k"))
            .isEqualTo(1);

        assertThat(statement.getString(1))
            .isEqualTo(statement.getString("v"))
            .isEqualTo("test");

        boolean threw = false;
        try {
            statement.getString(0);
        } catch (InvalidTypeException e) {
            threw = true;
        }
        assertThat(threw).as("Expected type error").isTrue();

        threw = false;
        try {
            statement.getString(2);
        } catch (IndexOutOfBoundsException e) {
            threw = true;
        }
        assertThat(threw).as("Expected index error").isTrue();
    }
}