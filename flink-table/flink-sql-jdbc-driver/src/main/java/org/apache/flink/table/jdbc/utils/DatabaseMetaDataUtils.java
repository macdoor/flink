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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.jdbc.FlinkDatabaseMetaData;
import org.apache.flink.table.jdbc.FlinkResultSet;

import javax.annotation.Nullable;

import java.sql.DatabaseMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Utils to create catalog/schema results for {@link FlinkDatabaseMetaData}. */
public class DatabaseMetaDataUtils {
    private static final Column TABLE_CAT_COLUMN =
            Column.physical("TABLE_CAT", DataTypes.STRING().notNull());
    private static final Column TABLE_SCHEM_COLUMN =
            Column.physical("TABLE_SCHEM", DataTypes.STRING().notNull());
    private static final Column TABLE_CATALOG_COLUMN =
            Column.physical("TABLE_CATALOG", DataTypes.STRING());

    /**
     * Create result set for catalogs. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_CAT String => catalog name.
     * </ul>
     *
     * <p>The results are ordered by catalog name.
     *
     * @param statement The statement for database meta data
     * @param result The result for catalogs
     * @return a ResultSet object in which each row has a single String column that is a catalog
     *     name
     */
    public static FlinkResultSet createCatalogsResultSet(
            Statement statement, StatementResult result) {
        List<RowData> catalogs = new ArrayList<>();
        result.forEachRemaining(catalogs::add);
        catalogs.sort(Comparator.comparing(v -> v.getString(0)));

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(catalogs.iterator()),
                ResolvedSchema.of(TABLE_CAT_COLUMN));
    }

    /**
     * Create result set for schemas. The schema columns are:
     *
     * <ul>
     *   <li>TABLE_SCHEM String => schema name
     *   <li>TABLE_CATALOG String => catalog name (may be null)
     * </ul>
     *
     * <p>The results are ordered by TABLE_CATALOG and TABLE_SCHEM.
     *
     * @param statement The statement for database meta data
     * @param catalogs The catalog list
     * @param catalogSchemas The catalog with schema list
     * @return a ResultSet object in which each row is a schema description
     */
    public static FlinkResultSet createSchemasResultSet(
            Statement statement, List<String> catalogs, Map<String, List<String>> catalogSchemas) {
        List<RowData> schemaWithCatalogList = new ArrayList<>();
        List<String> catalogList = new ArrayList<>(catalogs);
        catalogList.sort(String::compareTo);
        for (String catalog : catalogList) {
            List<String> schemas = catalogSchemas.get(catalog);
            schemas.sort(String::compareTo);
            schemas.forEach(
                    s ->
                            schemaWithCatalogList.add(
                                    GenericRowData.of(
                                            StringData.fromString(s),
                                            StringData.fromString(catalog))));
        }

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(schemaWithCatalogList.iterator()),
                ResolvedSchema.of(TABLE_SCHEM_COLUMN, TABLE_CATALOG_COLUMN));
    }

    /**
     * Matches a value against a SQL LIKE pattern where {@code %} matches any sequence and {@code _}
     * matches a single character.
     */
    public static boolean matchesPattern(@Nullable String value, @Nullable String pattern) {
        if (pattern == null || "%".equals(pattern)) {
            return true;
        }
        if (value == null) {
            return false;
        }
        String regex = ("\\Q" + pattern + "\\E").replace("%", "\\E.*\\Q").replace("_", "\\E.\\Q");
        return value.matches(regex);
    }

    /** Maps a Flink type name string to a {@link Types} constant. */
    public static int flinkTypeToSqlType(String flinkType) {
        String type = normalizeTypeName(flinkType);

        if (type.equals("BOOLEAN")) {
            return Types.BOOLEAN;
        } else if (type.equals("TINYINT")) {
            return Types.TINYINT;
        } else if (type.equals("SMALLINT")) {
            return Types.SMALLINT;
        } else if (type.equals("INT") || type.equals("INTEGER")) {
            return Types.INTEGER;
        } else if (type.equals("BIGINT")) {
            return Types.BIGINT;
        } else if (type.equals("FLOAT")) {
            return Types.FLOAT;
        } else if (type.equals("DOUBLE")) {
            return Types.DOUBLE;
        } else if (type.startsWith("DECIMAL") || type.startsWith("NUMERIC")) {
            return Types.DECIMAL;
        } else if (type.startsWith("CHAR(")) {
            return Types.CHAR;
        } else if (type.startsWith("VARCHAR") || type.equals("STRING")) {
            return Types.VARCHAR;
        } else if (type.startsWith("BINARY(")) {
            return Types.BINARY;
        } else if (type.startsWith("VARBINARY") || type.equals("BYTES")) {
            return Types.VARBINARY;
        } else if (type.equals("DATE")) {
            return Types.DATE;
        } else if (type.startsWith("TIME")) {
            return Types.TIME;
        } else if (type.startsWith("TIMESTAMP")) {
            return Types.TIMESTAMP;
        } else if (type.startsWith("ARRAY") || type.startsWith("MULTISET")) {
            return Types.ARRAY;
        } else if (type.startsWith("MAP")) {
            return Types.JAVA_OBJECT;
        } else if (type.startsWith("ROW")) {
            return Types.STRUCT;
        }
        return Types.OTHER;
    }

    /** Returns the column display size for a Flink type name string. */
    public static int flinkTypeColumnSize(String flinkType) {
        String type = normalizeTypeName(flinkType);

        if (type.equals("BOOLEAN")) {
            return 5;
        } else if (type.equals("TINYINT")) {
            return 3;
        } else if (type.equals("SMALLINT")) {
            return 5;
        } else if (type.equals("INT") || type.equals("INTEGER")) {
            return 10;
        } else if (type.equals("BIGINT")) {
            return 19;
        } else if (type.equals("FLOAT")) {
            return 7;
        } else if (type.equals("DOUBLE")) {
            return 15;
        } else if (type.equals("DATE")) {
            return 10;
        } else if (type.startsWith("TIME(")) {
            return 8 + parseFirstParam(type, 0);
        } else if (type.equals("TIME")) {
            return 8;
        } else if (type.startsWith("TIMESTAMP")) {
            return 29;
        } else if (type.equals("STRING")) {
            return Integer.MAX_VALUE;
        }

        int first = parseFirstParam(type, 0);
        if (first > 0) {
            return first;
        }
        return 0;
    }

    /** Returns the number of fractional digits for a Flink type name string. */
    public static int flinkTypeDecimalDigits(String flinkType) {
        String type = normalizeTypeName(flinkType);
        if (type.startsWith("DECIMAL") || type.startsWith("NUMERIC")) {
            return parseSecondParam(type, 0);
        }
        return 0;
    }

    private static String normalizeTypeName(String flinkType) {
        return flinkType.trim().toUpperCase().replace(" NOT NULL", "");
    }

    private static int parseFirstParam(String type, int defaultValue) {
        int start = type.indexOf('(');
        if (start < 0) {
            return defaultValue;
        }
        int end = type.indexOf(')', start);
        if (end < 0) {
            return defaultValue;
        }
        String params = type.substring(start + 1, end);
        try {
            return Integer.parseInt(params.split(",")[0].trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static int parseSecondParam(String type, int defaultValue) {
        int start = type.indexOf('(');
        if (start < 0) {
            return defaultValue;
        }
        int end = type.indexOf(')', start);
        if (end < 0) {
            return defaultValue;
        }
        String[] parts = type.substring(start + 1, end).split(",");
        if (parts.length < 2) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(parts[1].trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Create result set for tables. Each entry is {@code [TABLE_CAT, TABLE_SCHEM, TABLE_NAME,
     * TABLE_TYPE, REMARKS]}. The results are ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM,
     * TABLE_NAME.
     */
    public static FlinkResultSet createTablesResultSet(Statement statement, List<String[]> tables) {
        tables.sort(
                (a, b) -> {
                    int cmp = a[3].compareTo(b[3]);
                    if (cmp != 0) {
                        return cmp;
                    }
                    cmp = a[0].compareTo(b[0]);
                    if (cmp != 0) {
                        return cmp;
                    }
                    cmp = a[1].compareTo(b[1]);
                    if (cmp != 0) {
                        return cmp;
                    }
                    return a[2].compareTo(b[2]);
                });

        List<RowData> rows = new ArrayList<>();
        for (String[] t : tables) {
            rows.add(
                    GenericRowData.of(
                            StringData.fromString(t[0]),
                            StringData.fromString(t[1]),
                            StringData.fromString(t[2]),
                            StringData.fromString(t[3]),
                            StringData.fromString(t[4])));
        }

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(rows.iterator()),
                ResolvedSchema.of(
                        Column.physical("TABLE_CAT", DataTypes.STRING()),
                        Column.physical("TABLE_SCHEM", DataTypes.STRING()),
                        Column.physical("TABLE_NAME", DataTypes.STRING().notNull()),
                        Column.physical("TABLE_TYPE", DataTypes.STRING().notNull()),
                        Column.physical("REMARKS", DataTypes.STRING())));
    }

    /** Create result set for table types. Returns a single row with TABLE_TYPE = "TABLE". */
    public static FlinkResultSet createTableTypesResultSet(Statement statement) {
        List<RowData> rows =
                Arrays.asList(
                        GenericRowData.of(StringData.fromString("TABLE")),
                        GenericRowData.of(StringData.fromString("VIEW")));

        return new FlinkResultSet(
                statement,
                new CollectionResultIterator(rows.iterator()),
                ResolvedSchema.of(Column.physical("TABLE_TYPE", DataTypes.STRING().notNull())));
    }

    private static final ResolvedSchema COLUMNS_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("TABLE_CAT", DataTypes.STRING()),
                    Column.physical("TABLE_SCHEM", DataTypes.STRING()),
                    Column.physical("TABLE_NAME", DataTypes.STRING().notNull()),
                    Column.physical("COLUMN_NAME", DataTypes.STRING().notNull()),
                    Column.physical("DATA_TYPE", DataTypes.INT().notNull()),
                    Column.physical("TYPE_NAME", DataTypes.STRING().notNull()),
                    Column.physical("COLUMN_SIZE", DataTypes.INT()),
                    Column.physical("BUFFER_LENGTH", DataTypes.INT()),
                    Column.physical("DECIMAL_DIGITS", DataTypes.INT()),
                    Column.physical("NUM_PREC_RADIX", DataTypes.INT()),
                    Column.physical("NULLABLE", DataTypes.INT().notNull()),
                    Column.physical("REMARKS", DataTypes.STRING()),
                    Column.physical("COLUMN_DEF", DataTypes.STRING()),
                    Column.physical("SQL_DATA_TYPE", DataTypes.INT()),
                    Column.physical("SQL_DATETIME_SUB", DataTypes.INT()),
                    Column.physical("CHAR_OCTET_LENGTH", DataTypes.INT()),
                    Column.physical("ORDINAL_POSITION", DataTypes.INT().notNull()),
                    Column.physical("IS_NULLABLE", DataTypes.STRING().notNull()),
                    Column.physical("SCOPE_CATALOG", DataTypes.STRING()),
                    Column.physical("SCOPE_SCHEMA", DataTypes.STRING()),
                    Column.physical("SCOPE_TABLE", DataTypes.STRING()),
                    Column.physical("SOURCE_DATA_TYPE", DataTypes.SMALLINT()),
                    Column.physical("IS_AUTOINCREMENT", DataTypes.STRING().notNull()),
                    Column.physical("IS_GENERATEDCOLUMN", DataTypes.STRING().notNull()));

    /**
     * Build a single column row for the getColumns result set.
     *
     * @param catalog catalog name
     * @param schema schema (database) name
     * @param table table name
     * @param columnName column name
     * @param flinkTypeName the Flink type string (e.g. "INT", "VARCHAR(100)")
     * @param nullable whether the column is nullable
     * @param ordinalPosition 1-based ordinal position
     * @param isGenerated whether the column is a computed/generated column
     */
    public static GenericRowData buildColumnRow(
            String catalog,
            String schema,
            String table,
            String columnName,
            String flinkTypeName,
            boolean nullable,
            int ordinalPosition,
            boolean isGenerated) {
        int sqlType = flinkTypeToSqlType(flinkTypeName);
        int columnSize = flinkTypeColumnSize(flinkTypeName);
        int decimalDigits = flinkTypeDecimalDigits(flinkTypeName);
        int nullableFlag =
                nullable ? DatabaseMetaData.columnNullable : DatabaseMetaData.columnNoNulls;

        GenericRowData row = new GenericRowData(24);
        row.setField(0, StringData.fromString(catalog));
        row.setField(1, StringData.fromString(schema));
        row.setField(2, StringData.fromString(table));
        row.setField(3, StringData.fromString(columnName));
        row.setField(4, sqlType);
        row.setField(5, StringData.fromString(normalizeTypeName(flinkTypeName)));
        row.setField(6, columnSize);
        row.setField(7, null);
        row.setField(8, decimalDigits);
        row.setField(9, 10);
        row.setField(10, nullableFlag);
        row.setField(11, null);
        row.setField(12, null);
        row.setField(13, null);
        row.setField(14, null);
        row.setField(15, columnSize);
        row.setField(16, ordinalPosition);
        row.setField(17, StringData.fromString(nullable ? "YES" : "NO"));
        row.setField(18, null);
        row.setField(19, null);
        row.setField(20, null);
        row.setField(21, null);
        row.setField(22, StringData.fromString("NO"));
        row.setField(23, StringData.fromString(isGenerated ? "YES" : "NO"));
        return row;
    }

    /** Create result set for columns from pre-built rows. */
    public static FlinkResultSet createColumnsResultSet(
            Statement statement, List<RowData> columnRows) {
        return new FlinkResultSet(
                statement, new CollectionResultIterator(columnRows.iterator()), COLUMNS_SCHEMA);
    }

    private static final ResolvedSchema PRIMARY_KEYS_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("TABLE_CAT", DataTypes.STRING()),
                    Column.physical("TABLE_SCHEM", DataTypes.STRING()),
                    Column.physical("TABLE_NAME", DataTypes.STRING().notNull()),
                    Column.physical("COLUMN_NAME", DataTypes.STRING().notNull()),
                    Column.physical("KEY_SEQ", DataTypes.SMALLINT().notNull()),
                    Column.physical("PK_NAME", DataTypes.STRING()));

    /**
     * Create result set for primary keys. Each entry is {@code [TABLE_CAT, TABLE_SCHEM, TABLE_NAME,
     * COLUMN_NAME, KEY_SEQ (short), PK_NAME]}.
     */
    public static FlinkResultSet createPrimaryKeysResultSet(
            Statement statement, List<RowData> keyRows) {
        return new FlinkResultSet(
                statement, new CollectionResultIterator(keyRows.iterator()), PRIMARY_KEYS_SCHEMA);
    }

    /** Build a single primary key row for the getPrimaryKeys result set. */
    public static GenericRowData buildPrimaryKeyRow(
            String catalog, String schema, String table, String columnName, short keySeq) {
        return GenericRowData.of(
                StringData.fromString(catalog),
                StringData.fromString(schema),
                StringData.fromString(table),
                StringData.fromString(columnName),
                keySeq,
                null);
    }
}
