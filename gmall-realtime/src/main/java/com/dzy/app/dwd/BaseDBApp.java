package com.dzy.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.dzy.app.function.DimSinkFunction;
import com.dzy.app.function.TableProcessFunction;
import com.dzy.app.function.jsonDeserialization;
import com.dzy.bean.TableProcess;
import com.dzy.utils.kafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.消费kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app_2022";
        DataStreamSource<String> kafkaDS = env.addSource(kafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        kafkaDS.print("kafkaDS>>>>>>>>>>>>");
        //3.将每行数据转换称为JSON对象并过滤
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(line -> JSONObject.parseObject(line));
//                .filter(new FilterFunction<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                String data = value.getString("after");
//                return data!=null;
//            }
//        });
        //4.使用FlinkCDC消费配置表并处理成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("2019")
                .serverTimeZone("UTC")
                .databaseList("gmall-2022-realtime")
                .tableList("gmall-2022-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new jsonDeserialization())
                .build();

        DataStreamSource<String> processStrDS = env.addSource(sourceFunction);
        MapStateDescriptor mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state",String.class,TableProcess.class);
        BroadcastStream<String> broadcastStream = processStrDS.broadcast(mapStateDescriptor);
        
        //5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectDS = jsonDS.connect(broadcastStream);
        //6.处理数据 广播流数据
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag"){};
        SingleOutputStreamOperator<JSONObject> kafka = connectDS.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));
        //7.提取kafka流数据和Hbase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);
        hbase.print();
        //8.1 数据写入Hbase
        hbase.addSink(new DimSinkFunction());
        //8.2 数据写入kafka
        kafka.print("kafka>>>>>>>>>>>>>>");
        kafka.addSink(kafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sinkTable"),jsonObject.getString("after").getBytes());
            }
        }));
        //9.启动任务
        env.execute("BaseDBApp");
    }
}
