package org.myorg.quickstart;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

public class StreamingHive {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv.setParallelism(3);

        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, tableEnvSettings);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));

        String catalogName = "my_catalog";
        Catalog catalog = new HiveCatalog(
                catalogName,              // catalog name
                "default",                // default database
                "/Users/chenshuai/dev/apache-hive-2.3.4-bin/conf",  // Hive config (hive-site.xml) directory
                "2.3.6"                   // Hive version
        );
        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS stream_tmp");
        tableEnv.executeSql("DROP TABLE IF EXISTS stream_tmp.analytics_access_log_kafka");

        tableEnv.executeSql("CREATE TABLE stream_tmp.analytics_access_log_kafka (\n" +
                "       ts BIGINT,\n" +
                "       userId BIGINT,\n" +
                "       eventType STRING,\n" +
                "       fromType STRING,\n" +
                "       columnType STRING,\n" +
                "       siteId BIGINT,\n" +
                "       grouponId BIGINT,\n" +
                "       partnerId BIGINT,\n" +
                "       merchandiseId BIGINT,\n" +
                "       procTime AS PROCTIME(),\n" +
                "       eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "       WATERMARK FOR eventTime AS eventTime - INTERVAL '15' SECOND\n" +
                "     ) WITH (\n" +
                "       'connector' = 'kafka',\n" +
                "       'topic' = 'ods_analytics_access_log',\n" +
                "       'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "       'properties.group.id' = 'flink_hive_integration_exp_1',\n" +
                "       'scan.startup.mode' = 'latest-offset',\n" +
                "       'format' = 'json',\n" +
                "       'json.fail-on-missing-field' = 'false',\n" +
                "       'json.ignore-parse-errors' = 'true'\n" +
                "     )");

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS hive_tmp");
        tableEnv.executeSql("DROP TABLE IF EXISTS hive_tmp.analytics_access_log_hive");

        tableEnv.executeSql(" CREATE TABLE hive_tmp.analytics_access_log_hive (\n" +
                "       ts BIGINT,\n" +
                "       user_id BIGINT,\n" +
                "       event_type STRING,\n" +
                "       from_type STRING,\n" +
                "       column_type STRING,\n" +
                "       site_id BIGINT,\n" +
                "       groupon_id BIGINT,\n" +
                "       partner_id BIGINT,\n" +
                "       merchandise_id BIGINT\n" +
                "     ) PARTITIONED BY (\n" +
                "       ts_date STRING,\n" +
                "       ts_hour STRING,\n" +
                "       ts_minute STRING\n" +
                "     ) STORED AS PARQUET\n" +
                "     TBLPROPERTIES (\n" +
                "       'sink.partition-commit.trigger' = 'partition-time',\n" +
                "       'sink.partition-commit.delay' = '1 min',\n" +
                "       'sink.partition-commit.policy.kind' = 'metastore,success-file',\n" +
                "       'partition.time-extractor.timestamp-pattern' = '$ts_date $ts_hour:$ts_minute:00'\n" +
                "     )");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql(" INSERT INTO hive_tmp.analytics_access_log_hive\n" +
                "     SELECT\n" +
                "       ts,userId,eventType,fromType,columnType,siteId,grouponId,partnerId,merchandiseId,\n" +
                "       DATE_FORMAT(eventTime,'yyyy-MM-dd'),\n" +
                "       DATE_FORMAT(eventTime,'HH'),\n" +
                "       DATE_FORMAT(eventTime,'mm')\n" +
                "     FROM stream_tmp.analytics_access_log_kafka\n" +
                "     WHERE merchandiseId > 0");

        tableEnv.execute("StreamingHive");
//        streamEnv.execute("StreamingHive");

    }
}
