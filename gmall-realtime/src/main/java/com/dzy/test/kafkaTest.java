package com.dzy.test;

import com.dzy.utils.kafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class kafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stringDataStreamSource = env.addSource(kafkaUtil.getKafkaConsumer("dwd_page_log", "my"));
        stringDataStreamSource.print(">>>>>");
        env.execute("kafkaTest");
    }
}
