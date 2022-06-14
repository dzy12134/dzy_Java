package com.dzy.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dzy.utils.kafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 行为数据的分流
 * 新老用户校验
 * 数据分别发到三个kafka消息队列中
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.消费ods_base_Log主题数据创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app_2022";
        DataStreamSource<String> KafkaDS = env.addSource(kafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        //3.将每行数据转换成JSON对象
        //3.1脏数据标记
        OutputTag<String> outputTag = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = KafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //发生异常则写入侧输出流
                    context.output(outputTag,s);
                }
            }
        });
        jsonObjDS.getSideOutput(outputTag).print("dirty>>>>>>>>>");
        //4.新老用户校验（状态编程）
        //4.1 根据mid分组
        SingleOutputStreamOperator<JSONObject> JsonWithNewIdDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        //1.获取数据中的is_new标记
                        String is_new = jsonObject.getJSONObject("common").getString("is_new");
                        //2.判断is_new标记是否为1
                        if ("1".equals(is_new)) {
                            //获取状态标记
                            String state = valueState.value();
                            if (state != null) {
                                //修改is_new标记
                                jsonObject.getJSONObject("common").put("is_new", 0);
                                return jsonObject;
                            } else {
                                valueState.update("1");
                            }
                        } else {
                            return jsonObject;
                        }
                        return jsonObject;
                    }
                });
        //5.侧输出流
        //5.1 启动日志的tag和曝光数据tag
        OutputTag<String> startOutputTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        SingleOutputStreamOperator<String> pageDS = JsonWithNewIdDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                //1.启动日志字段
                String start = jsonObject.getString("start");
                if (start != null && start.length() != 0) {
                    //将数据写入启动日志侧输出流
                    context.output(startOutputTag, jsonObject.toJSONString());
                } else {
                    //页面数据写入主流日志
                    collector.collect(jsonObject.toJSONString());
                    //取出页面数据的曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() != 0) {
                        //page_id
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        //数据展平
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面id
                            display.put("page_id", pageId);
                            //数据写入曝光侧输出流
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                }
            }
        });
        //6.提取侧输出流的数据
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        //7.将三个流分别发入Kafka主题
        startDS.print("Start>>>>>>>>>>>");
        pageDS.print("Page>>>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>>");
        startDS.addSink(kafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(kafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(kafkaUtil.getKafkaProducer("dwd_display_log"));
        //8.启动任务
        env.execute("BaseLogApp");
    }
}
