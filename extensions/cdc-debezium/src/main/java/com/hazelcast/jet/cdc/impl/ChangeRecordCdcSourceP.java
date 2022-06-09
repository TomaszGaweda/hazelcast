/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cdc.impl;

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.core.EventTimePolicy;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Properties;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class ChangeRecordCdcSourceP extends CdcSourceP<ChangeRecord> {

    public static final String DB_SPECIFIC_EXTRA_FIELDS_PROPERTY = "db.specific.extra.fields";

    private final SequenceExtractor sequenceExtractor;

    public ChangeRecordCdcSourceP(
            @Nonnull Properties properties,
            @Nonnull EventTimePolicy<? super ChangeRecord> eventTimePolicy
    ) {
        super(properties, eventTimePolicy);

        try {
            sequenceExtractor = newInstance(properties.getProperty(SEQUENCE_EXTRACTOR_CLASS_PROPERTY),
                    "sequence extractor ");
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Nullable
    @Override
    protected ChangeRecord map(SourceRecord record) {
        if (record == null) {
            return null;
        }

        long sequenceSource = sequenceExtractor.source(record.sourcePartition(), record.sourceOffset());
        long sequenceValue = sequenceExtractor.sequence(record.sourceOffset());
        String keyJson = Values.convertToString(record.keySchema(), record.key());
        Struct value = (Struct) record.value();
        Struct source = (Struct) value.get("source");

        Operation operation = Operation.get(value.getString("op"));
        Schema valueSchema = record.valueSchema();

        Object before = value.get("before");
        Object after = value.get("after");
        Supplier<String> oldValueJson = before == null ? null : () -> Values.convertToString(valueSchema.field("before").schema(), before);
        Supplier<String> newValueJson = after == null ? null : () -> Values.convertToString(valueSchema.field("after").schema(), after);
        return new ChangeRecordImpl(
                value.getInt64("ts_ms"),
                sequenceSource,
                sequenceValue,
                operation,
                keyJson,
                oldValueJson,
                newValueJson,
                fieldOrNull(source, "table"),
                fieldOrNull(source, "schema"),
                fieldOrNull(source, "db")
        );
    }

    private static String fieldOrNull(Struct struct, String fieldName) {
        return struct.schema().field(fieldName) != null
                ? struct.getString(fieldName)
                : null;
    }
}
