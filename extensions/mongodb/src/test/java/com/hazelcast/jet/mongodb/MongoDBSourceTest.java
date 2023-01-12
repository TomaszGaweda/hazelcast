/*
 * Copyright 2021 Hazelcast Inc.
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

import com.google.common.collect.Sets;
import com.hazelcast.collection.IList;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.mongodb.MongoDBSourceBuilder.Batch;
import com.hazelcast.jet.mongodb.MongoDBSourceBuilder.Stream;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.mongodb.Mappers.streamToClass;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class})
public class MongoDBSourceTest extends AbstractMongoDBTest {

    @Parameter(0)
    public boolean filter;
    @Parameter(1)
    public boolean projection;
    @Parameter(2)
    public boolean sort;
    @Parameter(3)
    public boolean map;

    @Parameters(name = "filter:{0} | projection: {1} | sort: {2} | map: {3}")
    public static Object[] filterProjectionSortMatrix() {
        Set<Boolean> booleans = new HashSet<>(asList(true, false));
        return Sets.cartesianProduct(booleans, booleans, booleans, booleans).stream()
                   .map(tuple -> tuple.toArray(new Object[0]))
                   .toArray(Object[]::new);
    }

    @Test
    public void testBatchOneCollection() {
        IList<Object> list = instance().getList(testName.getMethodName());

        collection().insertMany(range(0, 30).mapToObj(i -> newDocument("key", i).append("val", i)).collect(toList()));
        assertEquals(collection().countDocuments(), 30L);

        Pipeline pipeline = Pipeline.create();
        String connectionString = mongoContainer.getConnectionString();
        Batch<?> sourceBuilder = MongoDBSources.batch(SOURCE_NAME, () -> mongoClient(connectionString))
                                                     .database(defaultDatabase())
                                                     .collection(testName.getMethodName(), Document.class);
        sourceBuilder = batchFilters(sourceBuilder);
        pipeline.readFrom(sourceBuilder.build())
                .setLocalParallelism(2)
                .writeTo(Sinks.list(list));

        instance().getJet().newJob(pipeline, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE)).join();

        assertTrueEventually(() -> contentAsserts(list, filter ? 10 : 0, 29, filter ? 20 : 30));
    }

    @Test
    public void testBatchDatabase() {
        IList<Object> list = instance().getList(testName.getMethodName());

        collection().insertMany(range(0, 20).mapToObj(i -> newDocument("key", i).append("val", i)).collect(toList()));
        assertEquals(collection().countDocuments(), 20L);

        collection(testName.getMethodName() + "_second")
                .insertMany(range(0, 20)
                        .mapToObj(i -> newDocument("key", i).append("val", i).append("test", "other"))
                        .collect(toList()));
        assertEquals(collection().countDocuments(), 20L);

        Pipeline pipeline = Pipeline.create();
        String connectionString = mongoContainer.getConnectionString();
        Batch<?> sourceBuilder = MongoDBSources.batch(SOURCE_NAME, () -> mongoClient(connectionString))
                .database(defaultDatabase());
        sourceBuilder = batchFilters(sourceBuilder);
        pipeline.readFrom(sourceBuilder.build())
                .setLocalParallelism(2)
                .peek()
                .writeTo(Sinks.list(list));

        instance().getJet().newJob(pipeline, new JobConfig().setProcessingGuarantee(EXACTLY_ONCE)).join();

        assertTrueEventually(() -> contentAsserts(list, filter ? 10 : 0, 19, filter ? 20 : 40));
    }

    private Batch<?> batchFilters(Batch<?> sourceBuilder) {
        if (filter) {
            sourceBuilder = sourceBuilder.filter(gte("key", 10));
        }
        if (projection) {
            sourceBuilder = sourceBuilder.project(include("val", "testName"));
        }
        if (sort) {
            sourceBuilder = sourceBuilder.sort(Sorts.ascending("val"));
        }
        if (map) {
            sourceBuilder = sourceBuilder.mapFn(Mappers.toClass(KV.class));
        }
        return sourceBuilder;
    }

    @Test
    public void testStreamOneCollection() {
        IList<Object> list = instance().getList(testName.getMethodName());
        String connectionString = mongoContainer.getConnectionString();
        Stream<?> sourceBuilder = MongoDBSourceBuilder.stream(SOURCE_NAME, () -> mongoClient(connectionString))
                                                      .database(defaultDatabase())
                                                      .collection(testName.getMethodName(), Document.class);
        sourceBuilder = streamFilters(sourceBuilder);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(sourceBuilder.build())
                .withNativeTimestamps(0)
                .setLocalParallelism(1)
                .writeTo(Sinks.list(list));

        Job job = instance().getJet().newJob(pipeline);

        collection().insertOne(newDocument("val", 1));
        collection().insertOne(newDocument("val", 10).append("foo", "bar"));
        collection("someOther").insertOne(newDocument("val", 1000).append("foo", "bar"));

        assertTrueEventually(() -> contentAsserts(list, filter ? 10 : 1, 10, filter ? 1 : 2));

        collection().insertOne(newDocument("val", 2));
        collection().insertOne(newDocument("val", 20).append("foo", "bar"));

        assertTrueEventually(() -> contentAsserts(list, filter ? 10 : 1, 20, filter ? 2 : 4));
        job.cancel();
    }

    @Test
    public void testStream_whenWatchDatabase() {
        IList<Object> list = instance().getList(testName.getMethodName());

        String connectionString = mongoContainer.getConnectionString();

        Stream<?> builder = MongoDBSourceBuilder
                .stream(SOURCE_NAME, () -> MongoClients.create(connectionString))
                .database(defaultDatabase());
        builder = streamFilters(builder);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(builder.build())
                .withNativeTimestamps(0)
                .setLocalParallelism(2)
                .writeTo(Sinks.list(list));

        instance().getJet().newJob(pipeline);

        MongoCollection<Document> col1 = collection("col1");
        MongoCollection<Document> col2 = collection("col2");

        col1.insertOne(newDocument("val", 1));
        col1.insertOne(newDocument("val", 10).append("foo", "bar"));

        col2.insertOne(newDocument("val", 2));
        col2.insertOne(newDocument("val", 11).append("foo", "bar"));

        assertTrueEventually(() -> contentAsserts(list, filter ? 10 : 1, 11, filter ? 2 : 4), 5);

        col1.insertOne(newDocument("val", 3));
        col1.insertOne(newDocument("val", 12).append("foo", "bar"));

        col2.insertOne(newDocument("val", 4));
        col2.insertOne(newDocument("val", 13).append("foo", "bar"));

        assertTrueEventually(() -> contentAsserts(list, filter ? 10 : 1, 13, filter ? 4 : 8), 5);

    }

    @Test
    public void testStream_whenWatchAll() {
        sleepSeconds(1);
        IList<Object> list = instance().getList(testName.getMethodName());

        String connectionString = mongoContainer.getConnectionString();

        Stream<?> builder = MongoDBSourceBuilder.stream(SOURCE_NAME, () -> MongoClients.create(connectionString));
        builder = streamFilters(builder);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(builder.build())
                .withNativeTimestamps(0)
                .setLocalParallelism(2)
                .peek()
                .writeTo(Sinks.list(list));

        Job job = instance().getJet().newJob(pipeline);

        MongoCollection<Document> col1 = collection("testDb1", "col1");
        MongoCollection<Document> col2 = collection("testDb1", "col2");
        MongoCollection<Document> col3 = collection("testDb2", "col3");

        col1.insertOne(newDocument("val", 1));
        col1.insertOne(newDocument("val", 11).append("foo", "bar").append("testName", testName.getMethodName()));
        col2.insertOne(newDocument("val", 2));
        col2.insertOne(newDocument("val", 12).append("foo", "bar").append("testName", testName.getMethodName()));
        col3.insertOne(newDocument("val", 3).append("testName", testName.getMethodName()));
        col3.insertOne(newDocument("val", 13).append("foo", "bar").append("testName", testName.getMethodName()));

        assertTrueEventually(() -> contentAsserts(list, filter ? 11 : 1, 13, filter ? 3 : 6), 5);

        col1.insertOne(newDocument("val", 4).append("testName", testName.getMethodName()));
        col1.insertOne(newDocument("val", 14).append("foo", "bar").append("testName", testName.getMethodName()));
        col2.insertOne(newDocument("val", 5).append("test", testName.getMethodName()));
        col2.insertOne(newDocument("val", 15).append("foo", "bar").append("testName", testName.getMethodName()));
        col2.insertOne(newDocument("val", 6));
        col2.insertOne(newDocument("val", 16).append("foo", "bar").append("testName", testName.getMethodName()));

        assertTrueEventually(() -> contentAsserts(list, filter ? 11  : 1, 16, filter ? 6 : 12));

        job.cancel();
    }

    private Document newDocument(String key1, Object val1) {
        return new Document(key1, val1).append("testName", testName.getMethodName());
    }

    @Nonnull
    private Stream<?> streamFilters(Stream<?> sourceBuilder) {
        if (filter) {
            sourceBuilder = sourceBuilder.filter(and(
                    gte("fullDocument.val", 10),
                    eq("operationType", "insert")
            ));
        } else {
            sourceBuilder = sourceBuilder.filter(eq("operationType", "insert"));
        }
        if (projection) {
            sourceBuilder = sourceBuilder.project(include("fullDocument.val", "fullDocument.testName"));
        }
        if (map) {
            sourceBuilder = sourceBuilder.mapFn(streamToClass(KV.class));
        }
        sourceBuilder = sourceBuilder.startAtOperationTime(new BsonTimestamp(System.currentTimeMillis()));
        return sourceBuilder;
    }

    private void contentAsserts(IList<Object> list, int expectedFirst, int expectedLast, int expectedSize) {
        List<Object> local = new ArrayList<>(list);
        String testName = this.testName.getMethodName();
        if (map) {
            local.removeIf(e -> !testName.equals(((KV) e).testName));
            local.sort(comparingInt(e -> ((KV) e).val));

            assertEquals(expectedSize, local.size());
            KV actualFirst = (KV) local.get(0);
            KV actualLast = (KV) local.get(local.size() - 1);
            if (projection) {
                assertNull(actualFirst.key);
            }
            assertEquals(expectedFirst, actualFirst.val.intValue());
            assertEquals(expectedLast, actualLast.val.intValue());
        } else {
            local.removeIf(e -> !testName.equals(((Document) e).get("testName")));
            local.sort(comparingInt(e -> ((Document) e).getInteger("val")));

            assertEquals(expectedSize, local.size());
            Document actualFirst = (Document) local.get(0);
            Document actualLast = (Document) local.get(local.size() - 1);
            if (projection) {
                assertNull(actualFirst.get("key"));
            }
            assertEquals(expectedFirst, actualFirst.getInteger("val").intValue());
            assertEquals(expectedLast, actualLast.getInteger("val").intValue());
        }
    }

    @SuppressWarnings("unused") // getters/setters are for Mongo converter
    public static class KV {
        private Integer key;
        private Integer val;
        private String testName;

        public KV() {
        }

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public int getVal() {
            return val;
        }

        public void setVal(int val) {
            this.val = val;
        }

        public String getTestName() {
            return testName;
        }

        public void setTestName(String testName) {
            this.testName = testName;
        }
    }

}
