/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.cache.impl;

import com.hazelcast.cache.impl.AbstractCachePartitionIterable;
import com.hazelcast.cache.impl.ICacheInternal;
import com.hazelcast.client.impl.spi.ClientContext;

import javax.annotation.Nonnull;
import javax.cache.Cache;
import java.util.Iterator;

public class ClientCachePartitionIterable<K, V> extends AbstractCachePartitionIterable<K, V> {
    private final int partitionId;
    private final ICacheInternal<K, V> cacheProxy;
    private final ClientContext context;

    public ClientCachePartitionIterable(ICacheInternal<K, V> cacheProxy,
                                        ClientContext context,
                                        int fetchSize,
                                        int partitionId,
                                        boolean prefetchValues) {
        super(fetchSize, prefetchValues);
        this.cacheProxy = cacheProxy;
        this.context = context;
        this.partitionId = partitionId;
    }

    @Override
    @Nonnull
    public Iterator<Cache.Entry<K, V>> iterator() {
        return new ClientCachePartitionIterator<>(cacheProxy, context, fetchSize, partitionId, prefetchValues);
    }
}