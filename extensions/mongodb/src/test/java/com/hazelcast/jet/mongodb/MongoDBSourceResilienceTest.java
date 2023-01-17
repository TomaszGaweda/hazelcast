/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.mongodb;

import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM;
import static eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class}) // TODO #23348 move it to nightly test
public class MongoDBSourceResilienceTest extends SimpleTestInClusterSupport {
    private static final ILogger logger = Logger.getLogger(MongoDBSourceResilienceTest.class);
    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("mongo:6.0.3");

    @Rule
    public Network network = Network.newNetwork();
    @Rule
    public MongoDBContainer mongoContainer = new MongoDBContainer(DOCKER_IMAGE_NAME)
            .withExposedPorts(27017)
            .withNetwork(network)
            .withNetworkAliases("mongo")
            ;

    @Rule
    public ToxiproxyContainer  toxi = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
            .withNetwork(network);

    @Rule
    public TestName testName  = new TestName();

    private final Random random = new Random();

    @Test
    public void testStream_whenServerDown() {
        HazelcastInstance hz = createHazelcastInstance();
        HazelcastInstance serverToShutdown = createHazelcastInstance();
        JobRepository jobRepository = new JobRepository(hz);
        int itemCount = 10_000;

        final String databaseName = "shutdownTest";
        final String collectionName = "testStream_whenServerDown";
        final String connectionString = mongoContainer.getConnectionString();
        Pipeline pipeline = buildIngestPipeline(connectionString, "set", databaseName, collectionName);

        Job job = invokeJob(hz, pipeline);
        ISet<Integer> set = hz.getSet("set");

        spawnMongo(connectionString, mongoClient -> {
            MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);

            for (int i = 0; i < itemCount / 2; i++) {
                collection.insertOne(new Document("key", i));
            }
        });
        waitForFirstSnapshot(jobRepository, job.getId(), 30, false);
        assertTrueEventually(() -> assertGreaterOrEquals("should have some records", set.size(), 1));

        spawnMongo(connectionString, mongoClient -> {
            MongoCollection<Document> collection =
                    mongoClient.getDatabase(databaseName).getCollection(collectionName);

            for (int i = itemCount / 2; i < itemCount; i++) {
                collection.insertOne(new Document("key", i));
            }
        });

        serverToShutdown.shutdown();

        assertTrueEventually(() ->
            assertEquals(itemCount, set.size())
        );
    }

    @Test
    public void testSource_networkCutoff() throws IOException {
        sourceNetworkTest(false);
    }


    @Test
    public void testSource_networkTimeout() throws IOException {
        sourceNetworkTest(true);
    }

    private void sourceNetworkTest(boolean timeout) throws IOException {
        final String directConnectionString = mongoContainer.getConnectionString();
        HazelcastInstance hz = createHazelcastInstance();
        JobRepository jobRepository = new JobRepository(hz);
        createHazelcastInstance();

        final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxi.getHost(), toxi.getControlPort());
        final Proxy proxy = toxiproxyClient.createProxy("mongo", "0.0.0.0:8670", "mongo:27017");

        final String databaseName = "networkCutoff";
        final String collectionName = "testNetworkCutoff";
        final String connectionViaToxi = "mongodb://" + toxi.getHost() + ":" + toxi.getMappedPort(8670);
        Pipeline pipeline = buildIngestPipeline(connectionViaToxi, "networkTest", databaseName, collectionName);

        Job job = invokeJob(hz, pipeline);
        ISet<Integer> set = hz.getSet("networkTest");

        final int itemCount = 200;
        AtomicInteger totalCount = new AtomicInteger(0);
        AtomicBoolean shouldContinue = new AtomicBoolean(true);

        spawnMongo(directConnectionString, mongoClient -> {
            MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);

            for (int i = 0; i < itemCount; i++) {
                collection.insertOne(new Document("key", i));
                totalCount.incrementAndGet();
            }
        });
        waitForFirstSnapshot(jobRepository, job.getId(), 30, false);

        spawnMongo(directConnectionString, mongoClient -> {
            MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(collectionName);

            for (int i = itemCount; shouldContinue.get(); i++) {
                collection.insertOne(new Document("key", i));
                totalCount.incrementAndGet();
            }
        });
        logger.info("Injecting toxis");
        if (timeout) {
            proxy.toxics().timeout("TIMEOUT", UPSTREAM, 0);
            proxy.toxics().timeout("TIMEOUT_DOWNSTREAM", DOWNSTREAM, 0);
        } else {
            proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", DOWNSTREAM, 0);
            proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", UPSTREAM, 0);
        }
        sleep(10_000 + random.nextInt(2000));
        if (timeout) {
            proxy.toxics().get("TIMEOUT").remove();
            proxy.toxics().get("TIMEOUT_DOWNSTREAM").remove();
        } else {
            proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
            proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();
        }
        logger.info("Toxiing over");
        sleep(5_000 + random.nextInt(1500));
        shouldContinue.compareAndSet(true, false);

        assertTrueEventually(() ->
                assertEquals(totalCount.get(), set.size())
        );
    }

    @Nonnull
    private static Sink<Integer> getSetSink(String setName) {
        return SinkBuilder
                .sinkBuilder(setName + "Sink", c -> c.hazelcastInstance().getSet(setName))
                .<Integer>receiveFn(Set::add)
                .build();
    }

    @Nonnull
    private static Pipeline buildIngestPipeline(String mongoContainerConnectionString, String sinkName,
                                                String databaseName, String collectionName) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(MongoDBSourceBuilder
                        .stream("mongo source", () -> mongoClient(mongoContainerConnectionString))
                        .database(databaseName)
                        .collection(collectionName)
                        .mapFn(ChangeStreamDocument::getFullDocument)
                        .startAtOperationTime(new BsonTimestamp(System.currentTimeMillis()))
                        .build())
                .withNativeTimestamps(0)
                .setLocalParallelism(4)
                .map(doc -> doc.getInteger("key"))
                .writeTo(getSetSink(sinkName));
        return pipeline;
    }

    @Test
    public void testSink_networkCutoff() throws IOException {
        sinkNetworkTest(false);
    }


    @Test
    public void testSink_networkTimeout() throws IOException {
        sinkNetworkTest(true);
    }

    private void sinkNetworkTest(boolean timeout) throws IOException {
        Config conf = new Config();
        conf.addMapConfig(new MapConfig("*").setEventJournalConfig(new EventJournalConfig().setEnabled(true)));
        conf.getJetConfig().setEnabled(true);
        HazelcastInstance hz = createHazelcastInstance(conf);
        JobRepository jobRepository = new JobRepository(hz);
        createHazelcastInstance(conf);

        final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxi.getHost(), toxi.getControlPort());
        final Proxy proxy = toxiproxyClient.createProxy("mongo", "0.0.0.0:8670", "mongo:27017");

        final String databaseName = "networkCutoff";
        final String collectionName = "testNetworkCutoff";
        final String connectionViaToxi = "mongodb://" + toxi.getHost() + ":" + toxi.getMappedPort(8670);

        IMap<Integer, Integer> sourceMap = hz.getMap(testName.getMethodName());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.mapJournal(sourceMap, START_FROM_OLDEST))
                .withIngestionTimestamps()
                .map(doc -> new Document("dummy", "test")
                        .append("key", doc.getKey())
                        .append("value", doc.getValue())
                ).setLocalParallelism(2)
                .writeTo(MongoDBSinks.builder("mongoSink", Document.class, () -> mongoClient(connectionViaToxi))
                                     .into(databaseName, collectionName)
                                     .preferredLocalParallelism(2)
                                     .build());

        Job job = invokeJob(hz, pipeline);
        final AtomicBoolean continueCounting = new AtomicBoolean(true);
        final AtomicInteger counter = new AtomicInteger(0);
        spawn(() -> {
            while (continueCounting.get()) {
                int val = counter.incrementAndGet();
                sourceMap.put(val, val);
                sleep(random.nextInt(100));
            }
        });
        waitForFirstSnapshot(jobRepository, job.getId(), 30, true);
        sleep(1_000);

        logger.info("Injecting toxis");
        if (timeout) {
            proxy.toxics().timeout("TIMEOUT", UPSTREAM, 0);
            proxy.toxics().timeout("TIMEOUT_DOWNSTREAM", DOWNSTREAM, 0);
        } else {
            proxy.toxics().bandwidth("CUT_CONNECTION_DOWNSTREAM", DOWNSTREAM, 0);
            proxy.toxics().bandwidth("CUT_CONNECTION_UPSTREAM", UPSTREAM, 0);
        }
        sleep(4_000 + random.nextInt(1000));
        if (timeout) {
            proxy.toxics().get("TIMEOUT").remove();
            proxy.toxics().get("TIMEOUT_DOWNSTREAM").remove();
        } else {
            proxy.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
            proxy.toxics().get("CUT_CONNECTION_UPSTREAM").remove();
        }
        logger.info("Toxiing over");
        sleep(1_000 + random.nextInt(1500));
        continueCounting.compareAndSet(true, false);

        final String directConnectionString = mongoContainer.getConnectionString();
        MongoClient directClinent = MongoClients.create(directConnectionString);
        MongoCollection<Document> collection = directClinent.getDatabase(databaseName).getCollection(collectionName);
        assertTrueEventually(() ->
                assertEquals(counter.get(), collection.countDocuments())
        );
    }

    @Nonnull
    private static Job invokeJob(HazelcastInstance hz, Pipeline pipeline) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(EXACTLY_ONCE)
                 .setSnapshotIntervalMillis(1000);
        Job job = hz.getJet().newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, RUNNING);
        return job;
    }

    public static Future<?> spawnMongo(String connectionString, Consumer<MongoClient> task) {
        FutureTask<Runnable> futureTask = new FutureTask<>(() -> {
            try (MongoClient mongoClient = MongoClients.create(connectionString)) {
                task.accept(mongoClient);
            }
        }, null);
        new Thread(futureTask).start();
        return futureTask;
    }
    private static void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static MongoClient mongoClient(String connectionString) {
        MongoClientSettings settings = MongoClientSettings
                .builder()
                .retryReads(false)
                .retryWrites(false)
                .applyToSocketSettings(builder -> {
                    builder.connectTimeout(2, SECONDS);
                    builder.readTimeout(2, SECONDS);
                })
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(5, SECONDS))
                .build();

        return MongoClients.create(settings);
    }

}
