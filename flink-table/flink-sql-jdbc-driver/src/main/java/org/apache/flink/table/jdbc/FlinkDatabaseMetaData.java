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

package org.apache.flink.table.jdbc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createCatalogsResultSet;
import static org.apache.flink.table.jdbc.utils.DatabaseMetaDataUtils.createSchemasResultSet;

/** Implementation of {@link java.sql.DatabaseMetaData} for flink jdbc driver. */
public class FlinkDatabaseMetaData extends BaseDatabaseMetaData {
    private final String url;
    private final FlinkConnection connection;
    private final Statement statement;
    private final Executor executor;

    @VisibleForTesting
    protected FlinkDatabaseMetaData(String url, FlinkConnection connection, Statement statement) {
        this.url = url;
        this.connection = connection;
        this.statement = statement;
        this.executor = connection.getExecutor();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        try (StatementResult result = catalogs()) {
            return createCatalogsResultSet(statement, result);
        } catch (Exception e) {
            throw new SQLException("Get catalogs fail", e);
        }
    }

    private StatementResult catalogs() {
        return executor.executeStatement("SHOW CATALOGS");
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        try {
            String currentCatalog = connection.getCatalog();
            String currentDatabase = connection.getSchema();
            List<String> catalogList = new ArrayList<>();
            Map<String, List<String>> catalogSchemaList = new HashMap<>();
            try (StatementResult result = catalogs()) {
                while (result.hasNext()) {
                    String catalog = result.next().getString(0).toString();
                    connection.setCatalog(catalog);
                    getSchemasForCatalog(catalogList, catalogSchemaList, catalog, null);
                }
            }
            connection.setCatalog(currentCatalog);
            connection.setSchema(currentDatabase);

            return createSchemasResultSet(statement, catalogList, catalogSchemaList);
        } catch (Exception e) {
            throw new SQLException("Get schemas fail", e);
        }
    }

    private void getSchemasForCatalog(
            List<String> catalogList,
            Map<String, List<String>> catalogSchemaList,
            String catalog,
            @Nullable String schemaPattern)
            throws SQLException {
        catalogList.add(catalog);
        List<String> schemas = new ArrayList<>();
        try (StatementResult schemaResult = schemas()) {
            while (schemaResult.hasNext()) {
                String schema = schemaResult.next().getString(0).toString();
                if (schemaPattern == null || schema.contains(schemaPattern)) {
                    schemas.add(schema);
                }
            }
        }
        catalogSchemaList.put(catalog, schemas);
    }

    private StatementResult schemas() {
        return executor.executeStatement("SHOW DATABASES;");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        try {
            String currentCatalog = connection.getCatalog();
            String currentDatabase = connection.getSchema();
            List<String> catalogList = new ArrayList<>();
            Map<String, List<String>> catalogSchemaList = new HashMap<>();

            if (catalog != null && !catalog.isEmpty()) {
                connection.setCatalog(catalog);
                getSchemasForCatalog(catalogList, catalogSchemaList, catalog, schemaPattern);
            } else {
                try (StatementResult result = catalogs()) {
                    while (result.hasNext()) {
                        String cat = result.next().getString(0).toString();
                        connection.setCatalog(cat);
                        getSchemasForCatalog(catalogList, catalogSchemaList, cat, schemaPattern);
                    }
                }
            }

            connection.setCatalog(currentCatalog);
            connection.setSchema(currentDatabase);
            return createSchemasResultSet(statement, catalogList, catalogSchemaList);
        } catch (Exception e) {
            throw new SQLException("Get schemas fail", e);
        }
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getTables(
            String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        try {
            String currentCatalog = connection.getCatalog();
            String currentDatabase = connection.getSchema();

            Set<String> requestedTypes = null;
            if (types != null) {
                requestedTypes = new HashSet<>();
                for (String t : types) {
                    requestedTypes.add(t.toUpperCase());
                }
            }

            List<String[]> tableList = new ArrayList<>();
            List<String> catalogsToCheck = getCatalogsToCheck(catalog);

            for (String cat : catalogsToCheck) {
                connection.setCatalog(cat);
                List<String> schemasToCheck = getSchemasToCheck(schemaPattern);

                for (String schema : schemasToCheck) {
                    connection.setSchema(schema);

                    Set<String> viewNames = new HashSet<>();
                    if (requestedTypes == null || requestedTypes.contains("VIEW")) {
                        try (StatementResult viewResult = executor.executeStatement("SHOW VIEWS")) {
                            while (viewResult.hasNext()) {
                                String viewName = viewResult.next().getString(0).toString();
                                viewNames.add(viewName);
                                if (DatabaseMetaDataUtils.matchesPattern(
                                        viewName, tableNamePattern)) {
                                    tableList.add(new String[] {cat, schema, viewName, "VIEW", ""});
                                }
                            }
                        }
                    }

                    if (requestedTypes == null || requestedTypes.contains("TABLE")) {
                        try (StatementResult tableResult =
                                executor.executeStatement("SHOW TABLES")) {
                            while (tableResult.hasNext()) {
                                String tableName = tableResult.next().getString(0).toString();
                                if (viewNames.contains(tableName)) {
                                    continue;
                                }
                                if (DatabaseMetaDataUtils.matchesPattern(
                                        tableName, tableNamePattern)) {
                                    tableList.add(
                                            new String[] {cat, schema, tableName, "TABLE", ""});
                                }
                            }
                        }
                    }
                }
            }

            connection.setCatalog(currentCatalog);
            connection.setSchema(currentDatabase);
            return DatabaseMetaDataUtils.createTablesResultSet(statement, tableList);
        } catch (Exception e) {
            throw new SQLException("Get tables fail", e);
        }
    }

    @Override
    public ResultSet getColumns(
            String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        try {
            String currentCatalog = connection.getCatalog();
            String currentDatabase = connection.getSchema();

            List<RowData> columnRows = new ArrayList<>();
            List<String> catalogsToCheck = getCatalogsToCheck(catalog);

            for (String cat : catalogsToCheck) {
                connection.setCatalog(cat);
                List<String> schemasToCheck = getSchemasToCheck(schemaPattern);

                for (String schema : schemasToCheck) {
                    connection.setSchema(schema);
                    List<String> tablesToCheck = getTablesToCheck(tableNamePattern);

                    for (String tableName : tablesToCheck) {
                        describeTableColumns(cat, schema, tableName, columnNamePattern, columnRows);
                    }
                }
            }

            connection.setCatalog(currentCatalog);
            connection.setSchema(currentDatabase);
            return DatabaseMetaDataUtils.createColumnsResultSet(statement, columnRows);
        } catch (Exception e) {
            throw new SQLException("Get columns fail", e);
        }
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
            throws SQLException {
        try {
            String currentCatalog = connection.getCatalog();
            String currentDatabase = connection.getSchema();

            List<RowData> keyRows = new ArrayList<>();
            String cat = catalog != null ? catalog : currentCatalog;
            String db = schema != null ? schema : currentDatabase;

            connection.setCatalog(cat);
            connection.setSchema(db);

            try (StatementResult descResult =
                    executor.executeStatement(String.format("DESCRIBE `%s`", table))) {
                short keySeq = 1;
                while (descResult.hasNext()) {
                    RowData row = descResult.next();
                    String keyField = safeGetString(row, 3);
                    if ("PRI".equalsIgnoreCase(keyField)) {
                        String columnName = row.getString(0).toString();
                        keyRows.add(
                                DatabaseMetaDataUtils.buildPrimaryKeyRow(
                                        cat, db, table, columnName, keySeq++));
                    }
                }
            } catch (Exception e) {
                // Table might not support DESCRIBE; return empty result
            }

            connection.setCatalog(currentCatalog);
            connection.setSchema(currentDatabase);
            return DatabaseMetaDataUtils.createPrimaryKeysResultSet(statement, keyRows);
        } catch (Exception e) {
            throw new SQLException("Get primary keys fail", e);
        }
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return DatabaseMetaDataUtils.createTableTypesResultSet(statement);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return url;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    /** In flink null value will be used as low value for sort. */
    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return DriverInfo.DRIVER_NAME;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION;
    }

    @Override
    public String getDriverName() throws SQLException {
        return FlinkDriver.class.getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return DriverInfo.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion() {
        return DriverInfo.DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MINOR;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return DriverInfo.DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /** Flink sql is mixed case as sensitive. */
    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    /** Null value plus non-null in flink will be null result. */
    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "database";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    /** Catalog name appears at the start of full name. */
    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    // ---- Private helpers for metadata introspection ----

    private List<String> getCatalogsToCheck(@Nullable String catalog) throws SQLException {
        List<String> result = new ArrayList<>();
        if (catalog != null && !catalog.isEmpty()) {
            result.add(catalog);
        } else {
            try (StatementResult catResult = catalogs()) {
                while (catResult.hasNext()) {
                    result.add(catResult.next().getString(0).toString());
                }
            }
        }
        return result;
    }

    private List<String> getSchemasToCheck(@Nullable String schemaPattern) {
        List<String> result = new ArrayList<>();
        try (StatementResult schemaResult = schemas()) {
            while (schemaResult.hasNext()) {
                String schema = schemaResult.next().getString(0).toString();
                if (DatabaseMetaDataUtils.matchesPattern(schema, schemaPattern)) {
                    result.add(schema);
                }
            }
        }
        return result;
    }

    private List<String> getTablesToCheck(@Nullable String tableNamePattern) {
        List<String> result = new ArrayList<>();
        try (StatementResult tableResult = executor.executeStatement("SHOW TABLES")) {
            while (tableResult.hasNext()) {
                String tableName = tableResult.next().getString(0).toString();
                if (DatabaseMetaDataUtils.matchesPattern(tableName, tableNamePattern)) {
                    result.add(tableName);
                }
            }
        }
        return result;
    }

    private void describeTableColumns(
            String catalog,
            String schema,
            String tableName,
            @Nullable String columnNamePattern,
            List<RowData> columnRows) {
        try (StatementResult descResult =
                executor.executeStatement(String.format("DESCRIBE `%s`", tableName))) {
            int ordinal = 1;
            while (descResult.hasNext()) {
                RowData row = descResult.next();
                String columnName = row.getString(0).toString();
                if (!DatabaseMetaDataUtils.matchesPattern(columnName, columnNamePattern)) {
                    ordinal++;
                    continue;
                }
                String typeName = row.getString(1).toString();
                boolean nullable = safeGetNullable(row, 2);
                String extras = safeGetString(row, 4);
                boolean isGenerated = extras != null && !extras.isEmpty();

                columnRows.add(
                        DatabaseMetaDataUtils.buildColumnRow(
                                catalog,
                                schema,
                                tableName,
                                columnName,
                                typeName,
                                nullable,
                                ordinal,
                                isGenerated));
                ordinal++;
            }
        } catch (Exception e) {
            // Skip tables that cannot be described
        }
    }

    /** Reads the nullable column which may be BOOLEAN or STRING depending on the gateway. */
    private static boolean safeGetNullable(RowData row, int index) {
        if (row.isNullAt(index)) {
            return true;
        }
        try {
            return row.getBoolean(index);
        } catch (ClassCastException e) {
            String val = row.getString(index).toString();
            return "true".equalsIgnoreCase(val) || "YES".equalsIgnoreCase(val);
        }
    }

    /** Reads a string field that may be null or non-string. */
    @Nullable
    private static String safeGetString(RowData row, int index) {
        if (row.isNullAt(index)) {
            return null;
        }
        try {
            return row.getString(index).toString();
        } catch (ClassCastException e) {
            return null;
        }
    }
}
