/*
 * Copyright 2024 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc.postgres;

import com.hazelcast.jet.sql.impl.connector.jdbc.AllTypesSelectJdbcSqlConnectorTest;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assumptions.assumeThat;

@Category(NightlyTest.class)
public class PostgresAllTypesSelectJdbcSqlConnectorTest extends AllTypesSelectJdbcSqlConnectorTest {

    @Parameterized.Parameters(name = "type:{0}, mappingType:{1}, value:{2}, expected:{3}")
    public static Collection<Object[]> parameters() {
        // Include parameters from the parent class
        Collection<Object[]> parentParams = AllTypesSelectJdbcSqlConnectorTest.parameters();

        // Add additional parameters in the child class
        List<Object[]> list = new ArrayList<>(parentParams);

        // BPCHAR pads string with blanks up to 10 chars
        Object[] additionalData = {"BPCHAR(10)", "VARCHAR", "'try '", "try       "};
        list.add(additionalData);

        return list;

    }
    @BeforeClass
    public static void beforeClass() {
        initialize(new PostgresDatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        assumeThat(type).describedAs("TINYINT not supported on Postgres")
                .isNotEqualTo("TINYINT");

        assumeThat(type).describedAs("TIMESTAMP WITH TIME ZONE not supported on Postgres")
                .isNotEqualTo("TIMESTAMP WITH TIME ZONE");


        if (type.equals("DOUBLE")) {
            type = "DOUBLE PRECISION";
        }
    }
}
