package com.dzy.app.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.LinkedHashMap;

public class jsonDeserialization implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        /*
        *封装为json数据格式
        * {
        *   "database":"",
        *   "tableName":"",
        *   "type":"cud",
        *   "before":"{"":"",}",
        *   "after":"{"":"","":""}"
        * }
         */
        //1.创建json对象
        JSONObject tablejosn = new JSONObject();
        //2.库名-表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String DataBase = split[1];
        String tableName = split[2];
        //3.before数据
        Struct value = (Struct)sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if(before != null){
            Schema beforeSchema = before.schema();
            for (Field field : beforeSchema.fields()) {
                beforeJson.put(field.name(),before.get(field));
            }
        }
        //4.after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if(after != null){
            Schema afterSchema = after.schema();
            for (Field field : afterSchema.fields()) {
                afterJson.put(field.name(),after.get(field));
            }
        }
        //5.操作op
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if("create".equals(type)){
            type = "insert";
        }

        //6.写入json中
        tablejosn.put("database",DataBase);
        tablejosn.put("tableName",tableName);
        tablejosn.put("before",beforeJson);
        tablejosn.put("after",afterJson);
        tablejosn.put("type",type);

        //7.输出数据
        collector.collect(tablejosn.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
