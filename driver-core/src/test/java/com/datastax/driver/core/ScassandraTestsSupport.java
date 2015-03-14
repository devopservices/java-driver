package com.datastax.driver.core;

import org.scassandra.Scassandra;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class ScassandraTestsSupport {

    protected static Scassandra scassandra;

    protected static Cluster cluster;

    protected static Session session;

    protected static PrimingClient primingClient;

    protected static ActivityClient activityClient;

    @BeforeClass(groups = { "short", "long" })
    public void createCluster() {
        scassandra = TestUtils.createScassandraServer();
        scassandra.start();
        Cluster.Builder builder = Cluster.builder()
            .addContactPoint("127.0.0.1")
            .withPoolingOptions(new PoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
                .setHeartbeatIntervalSeconds(0))
            .withPort(scassandra.getBinaryPort());
        builder = configure(builder);
        cluster = builder.build();
        session = cluster.connect();
        primingClient = scassandra.primingClient();
        activityClient = scassandra.activityClient();
    }

    @BeforeMethod
    @AfterMethod
    public void resetScassandraClients() {
        activityClient.clearAllRecordedActivity();
        primingClient.clearAllPrimes();
    }

    @AfterClass(groups = { "short", "long" })
    public void discardCluster() {
        if (cluster != null)
            cluster.close();
        if(scassandra != null)
            scassandra.stop();
    }

    /**
     * Give individual tests a chance to customize the cluster configuration.
     */
    protected Cluster.Builder configure(Cluster.Builder builder) {
        return builder;
    }

}