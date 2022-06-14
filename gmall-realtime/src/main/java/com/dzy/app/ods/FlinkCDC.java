package com.dzy.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.dzy.app.function.jsonDeserialization;
import com.dzy.utils.kafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        //1.1 开启CK，并指定状态后端为FS
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/gmall-flink-2022/ck"));
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
////        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,3,3));

        //2.通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("2019")
                .databaseList("gmall-2022-flink")
//                .tableList("gmall-2022-flink.base_trademark")
                .deserializer(new jsonDeserialization())
                .startupOptions(StartupOptions.latest())
                .serverTimeZone("UTC")
                .build();
        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);
        //3.写入kafka
        stringDataStreamSource.print();
        String sinkTopic = "ods_base_db";
        stringDataStreamSource.addSink(kafkaUtil.getKafkaProducer(sinkTopic));
        //4.任务
        env.execute("flinkcdc");
    }
}
