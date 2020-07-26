package org.myorg.quickstart;

import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class BatchReadWriteHive {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // to use hive dialect
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // to use default dialect
//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir     = "/Users/chenshuai/dev/apache-hive-2.3.4-bin/conf"; // a local path
        String version         = "2.3.4";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);

// set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");

//        TableResult tableResult1 = tableEnv.executeSql("select * from u_user");
//        tableResult1.print();
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("DROP TABLE IF EXISTS u_user_stats");

        tableEnv.executeSql(" CREATE TABLE u_user_stats (\n" +
                "       occupation STRING,\n" +
                "       gender STRING,\n" +
                "       ucount BIGINT\n" +
                "     ) STORED AS PARQUET\n");

        tableEnv.executeSql("INSERT OVERWRITE u_user_stats SELECT occupation, gender, count(1) as ucount FROM u_user GROUP BY occupation, gender");


    }
}
