/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.jdbc.utils;

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link DatabaseMetaDataUtils}. */
public class DatabaseMetaDataUtilsTest {

    @Test
    public void testMatchesPattern() {
        assertTrue(DatabaseMetaDataUtils.matchesPattern("abc", null));
        assertTrue(DatabaseMetaDataUtils.matchesPattern("abc", "%"));
        assertTrue(DatabaseMetaDataUtils.matchesPattern("abc", "abc"));
        assertTrue(DatabaseMetaDataUtils.matchesPattern("abc", "a%"));
        assertTrue(DatabaseMetaDataUtils.matchesPattern("abc", "%c"));
        assertTrue(DatabaseMetaDataUtils.matchesPattern("abc", "a_c"));
        assertTrue(DatabaseMetaDataUtils.matchesPattern("abc", "%b%"));

        assertFalse(DatabaseMetaDataUtils.matchesPattern("abc", "xyz"));
        assertFalse(DatabaseMetaDataUtils.matchesPattern("abc", "ab"));
        assertFalse(DatabaseMetaDataUtils.matchesPattern("abc", "a__c"));
        assertFalse(DatabaseMetaDataUtils.matchesPattern(null, "abc"));
    }

    @Test
    public void testFlinkTypeToSqlType() {
        assertEquals(Types.BOOLEAN, DatabaseMetaDataUtils.flinkTypeToSqlType("BOOLEAN"));
        assertEquals(Types.TINYINT, DatabaseMetaDataUtils.flinkTypeToSqlType("TINYINT"));
        assertEquals(Types.SMALLINT, DatabaseMetaDataUtils.flinkTypeToSqlType("SMALLINT"));
        assertEquals(Types.INTEGER, DatabaseMetaDataUtils.flinkTypeToSqlType("INT"));
        assertEquals(Types.INTEGER, DatabaseMetaDataUtils.flinkTypeToSqlType("INTEGER"));
        assertEquals(Types.BIGINT, DatabaseMetaDataUtils.flinkTypeToSqlType("BIGINT"));
        assertEquals(Types.FLOAT, DatabaseMetaDataUtils.flinkTypeToSqlType("FLOAT"));
        assertEquals(Types.DOUBLE, DatabaseMetaDataUtils.flinkTypeToSqlType("DOUBLE"));
        assertEquals(Types.DECIMAL, DatabaseMetaDataUtils.flinkTypeToSqlType("DECIMAL(10,2)"));
        assertEquals(Types.VARCHAR, DatabaseMetaDataUtils.flinkTypeToSqlType("VARCHAR(100)"));
        assertEquals(Types.VARCHAR, DatabaseMetaDataUtils.flinkTypeToSqlType("STRING"));
        assertEquals(Types.CHAR, DatabaseMetaDataUtils.flinkTypeToSqlType("CHAR(5)"));
        assertEquals(Types.DATE, DatabaseMetaDataUtils.flinkTypeToSqlType("DATE"));
        assertEquals(Types.TIME, DatabaseMetaDataUtils.flinkTypeToSqlType("TIME"));
        assertEquals(Types.TIMESTAMP, DatabaseMetaDataUtils.flinkTypeToSqlType("TIMESTAMP(3)"));
        assertEquals(Types.TIMESTAMP, DatabaseMetaDataUtils.flinkTypeToSqlType("TIMESTAMP_LTZ(3)"));
        assertEquals(Types.ARRAY, DatabaseMetaDataUtils.flinkTypeToSqlType("ARRAY<INT>"));
        assertEquals(
                Types.JAVA_OBJECT, DatabaseMetaDataUtils.flinkTypeToSqlType("MAP<STRING, INT>"));
        assertEquals(
                Types.STRUCT, DatabaseMetaDataUtils.flinkTypeToSqlType("ROW<a INT, b STRING>"));

        // NOT NULL suffix should be stripped
        assertEquals(Types.INTEGER, DatabaseMetaDataUtils.flinkTypeToSqlType("INT NOT NULL"));
    }

    @Test
    public void testFlinkTypeColumnSize() {
        assertEquals(10, DatabaseMetaDataUtils.flinkTypeColumnSize("INT"));
        assertEquals(19, DatabaseMetaDataUtils.flinkTypeColumnSize("BIGINT"));
        assertEquals(15, DatabaseMetaDataUtils.flinkTypeColumnSize("DOUBLE"));
        assertEquals(10, DatabaseMetaDataUtils.flinkTypeColumnSize("DECIMAL(10,2)"));
        assertEquals(100, DatabaseMetaDataUtils.flinkTypeColumnSize("VARCHAR(100)"));
        assertEquals(5, DatabaseMetaDataUtils.flinkTypeColumnSize("CHAR(5)"));
        assertEquals(10, DatabaseMetaDataUtils.flinkTypeColumnSize("DATE"));
        assertEquals(29, DatabaseMetaDataUtils.flinkTypeColumnSize("TIMESTAMP(3)"));
        assertEquals(Integer.MAX_VALUE, DatabaseMetaDataUtils.flinkTypeColumnSize("STRING"));
    }

    @Test
    public void testFlinkTypeDecimalDigits() {
        assertEquals(0, DatabaseMetaDataUtils.flinkTypeDecimalDigits("INT"));
        assertEquals(2, DatabaseMetaDataUtils.flinkTypeDecimalDigits("DECIMAL(10,2)"));
        assertEquals(0, DatabaseMetaDataUtils.flinkTypeDecimalDigits("DECIMAL(10)"));
        assertEquals(5, DatabaseMetaDataUtils.flinkTypeDecimalDigits("NUMERIC(20,5)"));
    }
}
