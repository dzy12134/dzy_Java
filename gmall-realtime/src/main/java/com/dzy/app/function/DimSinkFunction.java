package com.dzy.app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.util.*;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("hbase.zookeeper.quorum","hadoop102:2181");
        this.connection = ConnectionFactory.createConnection(configuration);
        System.out.println("hbase>>>>>>>>连接成功");
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        JSONObject after = value.getJSONObject("after");
        Table table = connection.getTable(TableName.valueOf("dzy:"+value.getString("sinkTable")));
        String rowkey = after.getString("id");
        //单行添加数据
        Put row = new Put(rowkey.getBytes());
        for (Map.Entry<String, Object> entry : after.entrySet()) {
            if (entry.getKey().equals("id")){
                continue;
            }else {
                String key = entry.getKey();
                String va = (String) entry.getValue();
                row.addColumn("base".getBytes(),key.getBytes(),va.getBytes());
            }
        }
        table.put(row);
    }
}
