package com.dzy.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.dzy.utils.kafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境中，与kafka的分区数目保持一致
        //2.读取Kafka数据，主题 dwd_pagelogo
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(kafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        //3.将每行数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(line -> JSONObject.parseObject(line));
        //4.过滤数据 状态编程 只保留每个mid每天第一次登录的时间
        KeyedStream<JSONObject, String> keyedStream = jsonDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> dataState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
                dataState = getRuntimeContext().getState(stringValueStateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String last_page = jsonObject.getJSONObject("page").getString("last_page_id");
                if (last_page == null || last_page.length() <= 0){
                    //取出状态数据
                    String lastDate = dataState.value();
                    String ts = simpleDateFormat.format(jsonObject.getLong("ts"));
                    if ( !ts.equals(lastDate) ){
                        dataState.update(ts);
                        return true;
                    }
                }
                return false;
            }
        });
        //5.将数据写入Kafka
        uvDS.print("uvDS>>>>>>>>>>>>>>>>");
        uvDS.map(jsonObject -> jsonObject.toJSONString()).addSink(kafkaUtil.getKafkaProducer(sinkTopic));
        //6.执行
        env.execute("UniqueVisitApp");
    }
}
