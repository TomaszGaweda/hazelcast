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

package com.hazelcast.jet.sql;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlParameterTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    @Parameterized.Parameter
    public boolean useClient;

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance member;
    private HazelcastInstance client;

    @Parameterized.Parameters(name = "useClient:{0}")
    public static Object[] parameters() {
        return new Object[]{false, true};
    }

    @Before
    public void setUp() {
        member = factory.newHazelcastInstance();

        if (useClient) {
            client = factory.newHazelcastClient();
        }

        createMapping(member, MAP_NAME, int.class, int.class);
        member.getMap(MAP_NAME).put(1, 1);
    }

    @After
    public void tearDown() {
        member = null;
        client = null;

        factory.shutdownAll();
    }

    @Test
    public void testParameters() {
        boolean valBoolean = true;
        byte valByte = 1;
        short valShort = 2;
        int valInt = 3;
        long valLong = 4;
        float valFloat = 5;
        double valDouble = 6;
        BigDecimal valDecimal = BigDecimal.valueOf(7);
        String valString = "str";
        LocalDate valLocalDate = LocalDate.now();
        LocalTime valLocalTime = LocalTime.now();
        LocalDateTime valLocalDateTime = LocalDateTime.now();
        OffsetDateTime valOffsetDateTime = OffsetDateTime.now();
        CustomObject valObject = new CustomObject(1);

        HazelcastInstance target = useClient ? client : member;

        SqlStatement statement = new SqlStatement(
                "SELECT "
                + "CAST(? as BOOLEAN), "
                + "CAST(? as TINYINT), "
                + "CAST(? as SMALLINT), "
                + "CAST(? as INTEGER), "
                + "CAST(? as BIGINT), "
                + "CAST(? as REAL), "
                + "CAST(? as DOUBLE), "
                + "CAST(? as DECIMAL), "
                + "CAST(? as VARCHAR), "
                + "CAST(? as DATE), "
                + "CAST(? as TIME), "
                + "CAST(? as TIMESTAMP), "
                + "CAST(? as TIMESTAMP WITH TIME ZONE), "
                + "CAST(? as OBJECT), "
                + "CAST(? as OBJECT) "
                + "FROM " + MAP_NAME);

        statement.addParameter(valBoolean);
        statement.addParameter(valByte);
        statement.addParameter(valShort);
        statement.addParameter(valInt);
        statement.addParameter(valLong);
        statement.addParameter(valFloat);
        statement.addParameter(valDouble);
        statement.addParameter(valDecimal);
        statement.addParameter(valString);
        statement.addParameter(valLocalDate);
        statement.addParameter(valLocalTime);
        statement.addParameter(valLocalDateTime);
        statement.addParameter(valOffsetDateTime);
        statement.addParameter(valObject);
        statement.addParameter(null);

        try (SqlResult res = target.getSql().execute(statement)) {
            for (SqlRow row : res) {
                assertEquals(valBoolean, row.getObject(0));
                assertEquals(valByte, (byte) row.getObject(1));
                assertEquals(valShort, (short) row.getObject(2));
                assertEquals(valInt, (int) row.getObject(3));
                assertEquals(valLong, (long) row.getObject(4));
                assertEquals(valFloat, row.getObject(5), 0f);
                assertEquals(valDouble, row.getObject(6), 0d);
                assertEquals(valDecimal, row.getObject(7));
                assertEquals(valString, row.getObject(8));
                assertEquals(valLocalDate, row.getObject(9));
                assertEquals(valLocalTime, row.getObject(10));
                assertEquals(valLocalDateTime, row.getObject(11));
                assertEquals(valOffsetDateTime, row.getObject(12));
                assertEquals(valObject, row.getObject(13));
                assertNull(row.getObject(14));
            }
        }
    }

    public static class CustomObject implements Serializable {

        private final int id;

        public CustomObject(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CustomObject that = (CustomObject) o;

            return id == that.id;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }
}
