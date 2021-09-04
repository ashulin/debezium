/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.oracle.StreamingAdapter.TableNameCaseSensitivity;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSource;
import io.debezium.connector.oracle.xstream.LcrPosition;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

public class OracleConnectorTask extends BaseSourceTask<OraclePartition, OracleOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorTask.class);
    private static final String CONTEXT_NAME = "oracle-connector-task";

    private volatile OracleTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile OracleConnection jdbcConnection;
    private volatile ErrorHandler errorHandler;
    private volatile OracleDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> start(Configuration config) {
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        TopicSelector<TableId> topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        Configuration jdbcConfig = connectorConfig.getJdbcConfig();
        jdbcConnection = new OracleConnection(jdbcConfig, () -> getClass().getClassLoader());

        validateRedoLogConfiguration();

        OracleValueConverters valueConverters = new OracleValueConverters(connectorConfig, jdbcConnection);
        TableNameCaseSensitivity tableNameCaseSensitivity = connectorConfig.getAdapter().getTableNameCaseSensitivity(jdbcConnection);
        this.schema = new OracleDatabaseSchema(connectorConfig, valueConverters, schemaNameAdjuster, topicSelector, tableNameCaseSensitivity);
        this.schema.initializeStorage();
        Offsets<OraclePartition, OracleOffsetContext> previousOffsets = getPreviousOffsets(new OraclePartition.Provider(connectorConfig),
                connectorConfig.getAdapter().getOffsetContextLoader());

        LOGGER.info("Closing connection before starting schema recovery");

        try {
            jdbcConnection.close();
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        OraclePartition partition = previousOffsets.getTheOnlyPartition();
        OracleOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();

        validateAndLoadDatabaseHistory(connectorConfig, partition, previousOffset, schema);

        LOGGER.info("Reconnecting after finishing schema recovery");

        try {
            jdbcConnection.setAutoCommit(false);

        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        validateSnapshotFeasibility(connectorConfig, previousOffset);

        taskContext = new OracleTaskContext(connectorConfig, schema);

        Clock clock = Clock.system();

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        errorHandler = new OracleErrorHandler(connectorConfig.getLogicalName(), queue);

        final OracleEventMetadataProvider metadataProvider = new OracleEventMetadataProvider();

        EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                metadataProvider,
                schemaNameAdjuster);

        final OracleStreamingChangeEventSourceMetrics streamingMetrics = new OracleStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider,
                connectorConfig);

        ChangeEventSourceCoordinator<OraclePartition, OracleOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                OracleConnector.class,
                connectorConfig,
                new OracleChangeEventSourceFactory(connectorConfig, jdbcConnection, errorHandler, dispatcher, clock, schema, jdbcConfig, taskContext, streamingMetrics),
                new OracleChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();

        List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    public void doStop() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        schema.close();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return OracleConnectorConfig.ALL_FIELDS;
    }

    private void validateRedoLogConfiguration() {
        // Check whether the archive log is enabled.
        final boolean archivelogMode = jdbcConnection.isArchiveLogMode();
        if (archivelogMode) {
            throw new DebeziumException("The Oracle server is not configured to use a archive log LOG_MODE, which is "
                    + "required for this connector to work properly. Change the Oracle configuration to use a "
                    + "LOG_MODE=ARCHIVELOG and restart the connector.");
        }
    }

    private boolean validateAndLoadDatabaseHistory(OracleConnectorConfig config, OraclePartition partition, OracleOffsetContext offset, OracleDatabaseSchema schema) {
        if (offset == null) {
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError()) {
                // We are in schema only recovery mode, use the existing redo log position
                // would like to also verify redo log position exists, but it defaults to 0 which is technically valid
                throw new DebeziumException("Could not find existing redo log information while attempting schema only recovery snapshot");
            }
            LOGGER.info("Connector started for the first time, database history recovery will not be executed");
            schema.initializeStorage();
            return false;
        }
        if (!schema.historyExists()) {
            LOGGER.warn("Database history was not found but was expected");
            if (config.getSnapshotMode().shouldSnapshotOnSchemaError()) {
                // But check to see if the server still has those redo log coordinates ...
                if (!isRedoLogAvailable(config, offset)) {
                    throw new DebeziumException("The connector is trying to read redo log starting at " + offset.getSourceInfo() + ", but this is no longer "
                            + "available on the server. Reconfigure the connector to use a snapshot when needed.");
                }
                LOGGER.info("The db-history topic is missing but we are in {} snapshot mode. " +
                        "Attempting to snapshot the current schema and then begin reading the redo log from the last recorded offset.",
                        OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            else {
                throw new DebeziumException("The db history topic is missing. You may attempt to recover it by reconfiguring the connector to "
                        + OracleConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            }
            schema.initializeStorage();
            return true;
        }
        schema.recover(partition, offset);
        return false;
    }

    /**
     * Determine whether the redo log position as set on the {@link OracleOffsetContext} is available in the server.
     *
     * @return {@code true} if the server has the redo log coordinates, or {@code false} otherwise
     */
    protected boolean isRedoLogAvailable(OracleConnectorConfig config, OracleOffsetContext offset) {
        String lcrStr = offset.getLcrPosition();
        Scn scn;

        if (lcrStr != null) {
            scn = LcrPosition.valueOf(lcrStr).getScn();
        }
        else {
            scn = offset.getScn();
        }

        if (scn == null || scn.isNull()) {
            return false;
        }

        try {
            Scn oldestScn = LogMinerStreamingChangeEventSource.getFirstScnInLogs(jdbcConnection, config.getLogMiningArchiveLogRetention(),
                    config.getLogMiningArchiveDestinationName());
            Scn currentScn = jdbcConnection.getCurrentScn();
            // And compare with the one we're supposed to use.
            if (scn.compareTo(oldestScn) < 0 || scn.compareTo(currentScn) > 0) {
                LOGGER.info("Connector requires redo log scn '{}', but Oracle only has {}-{}", scn, oldestScn, currentScn);
                return false;
            }
            else {
                LOGGER.info("Oracle has the redo log scn '{}' required by the connector", scn);
                return true;
            }
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }

    private boolean validateSnapshotFeasibility(OracleConnectorConfig config, OracleOffsetContext offset) {
        if (offset != null) {
            if (!offset.isSnapshotRunning()) {
                // But check to see if the server still has those binlog coordinates ...
                if (!isRedoLogAvailable(config, offset)) {
                    throw new DebeziumException("The connector is trying to read binlog starting at " + offset.getSourceInfo() + ", but this is no longer "
                            + "available on the server. Reconfigure the connector to use a snapshot when needed.");
                }
            }
        }
        return false;
    }
}
