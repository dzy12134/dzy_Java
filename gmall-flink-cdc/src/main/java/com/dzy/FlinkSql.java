package com.dzy;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSql {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabelEnv = StreamTableEnvironment.create(env);
        //2.创建flink-mysqlcdc的source
        tabelEnv.executeSql("CREATE TABLE user_info("+
                "id STRING NOT NULL,"+
                "tm_name STRING,"+
                "logo_url STRING"+
                ") WITH ("+
                "'connector' = 'mysql-cdc',"+
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root',"+
                "'password' = '2019',"+
                "'database-name' = 'gmall-2022-flink',"+
                "'table-name' = 'base_trademark'"+
                ")");
        //3.查询数据，打印
        tabelEnv.executeSql("select * from user_info").print();
        tabelEnv.execute("FlinkSql");
    }
}
