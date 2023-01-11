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

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import com.hazelcast.jet.pipeline.Sinks;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.UpdateOptions;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * See {@link MongoDBSinks#builder}
 *
 * @param <T> type of the items the sink will accept
 */
public final class MongoDBSinkBuilder<T> {

    private final String name;
    private final SupplierEx<MongoClient> connectionSupplier;
    @Nonnull
    private final Class<T> documentClass;

    private int preferredLocalParallelism = 2;

    private FunctionEx<T, String> selectDatabaseNameFn;
    private FunctionEx<T, String> selectCollectionNameFn;
    private String databaseName;
    private String collectionName;
    private String idFieldName;
    private FunctionEx<T, Object> documentIdentityFn;
    private ConsumerEx<UpdateOptions> updateOptionsChanger;

    /**
     * See {@link MongoDBSinks#builder}
     */
    MongoDBSinkBuilder(
            @Nonnull String name,
            @Nonnull Class<T> documentClass,
            @Nonnull SupplierEx<MongoClient> connectionSupplier
    ) {
        this.documentClass = documentClass;
        checkSerializable(connectionSupplier, "connectionSupplier");
        this.name = name;
        this.connectionSupplier = connectionSupplier;
    }

    /**
     * @param selectDatabaseNameFn selects database name for each item individually
     * @param selectCollectionNameFn selects collection name for each item individually
     */
    public MongoDBSinkBuilder<T> into(
            @Nonnull FunctionEx<T, String> selectDatabaseNameFn,
            @Nonnull FunctionEx<T, String> selectCollectionNameFn
    ) {
        checkSerializable(selectDatabaseNameFn, "selectDatabaseNameFn");
        checkSerializable(selectCollectionNameFn, "selectCollectionNameFn");
        this.selectDatabaseNameFn = selectDatabaseNameFn;
        this.selectCollectionNameFn = selectCollectionNameFn;
        return this;
    }

    /**
     * @param databaseName database name to which objects will be inserted/updated.
     * @param collectionName collection name to which objects will be inserted/updated.
     */
    public MongoDBSinkBuilder<T> into(@Nonnull String databaseName, @Nonnull String collectionName) {
        checkNotNull(databaseName, "databaseName cannot be null");
        checkNotNull(collectionName, "collectionName cannot be null");
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        return this;
    }

    /**
     * See {@link SinkBuilder#preferredLocalParallelism(int)}.
     */
    public MongoDBSinkBuilder<T> preferredLocalParallelism(int preferredLocalParallelism) {
        this.preferredLocalParallelism = Vertex.checkLocalParallelism(preferredLocalParallelism);
        return this;
    }

    /**
     * Sets the filter that decides which document in the collection is equal to processed document.
     */
    public MongoDBSinkBuilder<T> identifyDocumentBy(@Nonnull String fieldName, @Nonnull FunctionEx<T, Object> documentIdentityFn) {
        checkNotNull(fieldName, "fieldName cannot be null");
        checkSerializable(documentIdentityFn, "documentIdentityFn");
        this.idFieldName = fieldName;
        this.documentIdentityFn = documentIdentityFn;
        return this;
    }

    /**
     * Sets the filter that decides which document in the collection is equal to processed document.
     */
    public MongoDBSinkBuilder<T> withUpdateOptionsChanged(@Nonnull ConsumerEx<UpdateOptions> updateOptionsChanger) {
        checkSerializable(updateOptionsChanger, "updateOptionsChanger");
        this.updateOptionsChanger = updateOptionsChanger;
        return this;
    }

    /**
     * Creates and returns the MongoDB {@link Sink} with the components you
     * supplied to this builder.
     */
    public Sink<T> build() {
        checkNotNull(connectionSupplier, "connectionSupplier must be set");
        checkNotNull(documentIdentityFn, "documentIdentityFn must be set");

        final SupplierEx<MongoClient> connectionSupplier = this.connectionSupplier;
        final Class<T> documentClass = this.documentClass;
        final String databaseName = this.databaseName;
        final String collectionName = this.collectionName;
        final FunctionEx<T, String> selectDatabaseNameFn = this.selectDatabaseNameFn;
        final FunctionEx<T, String> selectCollectionNameFn = this.selectCollectionNameFn;
        final FunctionEx<T, Object> documentIdentityFn = this.documentIdentityFn;
        final String fieldName = this.idFieldName;
        final ConsumerEx<UpdateOptions> updateOptionsChanger = this.updateOptionsChanger == null
                ? ConsumerEx.noop()
                : this.updateOptionsChanger;

        checkState((databaseName == null) == (collectionName == null), "if one of [databaseName, collectionName]" +
                " is provided, so should the other one");
        checkState((selectDatabaseNameFn == null) == (selectCollectionNameFn == null),
                "if one of [selectDatabaseNameFn, selectCollectionNameFn] is provided, so should the other one");

        checkState((selectDatabaseNameFn == null) != (databaseName == null),
                "Only select*Fn or *Name functions should be called, never mixed");

        if (databaseName != null) {
        return Sinks.fromProcessor(name, ProcessorMetaSupplier.of(preferredLocalParallelism,
                ProcessorSupplier.of(() -> new WriteMongoP<>(connectionSupplier, databaseName, collectionName,
                        documentClass, documentIdentityFn, updateOptionsChanger, fieldName))));
        } else {
            return Sinks.fromProcessor(name, ProcessorMetaSupplier.of(preferredLocalParallelism,
                    ProcessorSupplier.of(() -> new WriteMongoP<>(connectionSupplier, selectDatabaseNameFn,
                            selectCollectionNameFn, documentClass, documentIdentityFn, updateOptionsChanger, fieldName))));
        }
    }

}
