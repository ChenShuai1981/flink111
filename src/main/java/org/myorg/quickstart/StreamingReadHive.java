package org.myorg.quickstart;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

import java.time.Duration;

public class StreamingReadHive {

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

        tableEnv.getConfig().getConfiguration().setBoolean(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);

        Table result = tableEnv.sqlQuery(" SELECT merchandise_id,count(1) AS pv\n" +
                "      FROM hive_tmp.analytics_access_log_hive\n" +
                "      /*+ OPTIONS(\n" +
                "        'streaming-source.enable' = 'true',\n" +
                "        'streaming-source.monitor-interval' = '1 min',\n" +
                "        'streaming-source.consume-start-offset' = '2020-07-26 15:10:00'\n" +
                "      ) */\n" +
                "      WHERE event_type = 'pay'\n" +
                "      AND ts_date >= '2020-07-26'\n" +
                "      GROUP BY merchandise_id\n" +
                "      ORDER BY pv DESC LIMIT 10");

        tableEnv.toRetractStream(result, Row.class).print().setParallelism(1);

        streamEnv.execute("StreamingReadHive");

    }
}
