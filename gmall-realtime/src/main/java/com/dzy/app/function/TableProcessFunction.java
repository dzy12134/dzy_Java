package com.dzy.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dzy.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;
    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("hbase.zookeeper.quorum","hadoop102:2181");
        this.connection = ConnectionFactory.createConnection(configuration);
        System.out.println(connection);
        System.out.println("hbase>>>>>>>>连接成功");
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        String key = value.getString("tableName")+"-"+value.getString("type");
        //1.获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(key);
        if(tableProcess != null){
            //2.过滤字段
            JSONObject after = value.getJSONObject("after");
            filterColumn(after, tableProcess.getSink_columns());
            //3.分流
            value.put("sinkTable",tableProcess.getSink_table());
            String sinType = tableProcess.getSink_type();
            if(TableProcess.SINK_TYPE_KAFKA.equals(sinType)){
                //kafka写入主流
                collector.collect(value);
            }else if(TableProcess.SINK_TYPE_HBASE.equals(sinType)){
                //hbase写入侧输出流
                readOnlyContext.output(outputTag,value);
            }
        }else{
            System.out.println("该组合Key："+key+"不存在！");
        }
    }

    /**
     *
     * @param after : {"id":"1","tm_name":"dzy","log_url":"aaa"}
     * @param sinkColumns : id,tm_name
     */
    private void filterColumn(JSONObject after, String sinkColumns) {
        String[] split = sinkColumns.split(",");
        List<String> columns = Arrays.asList(split);
//        Iterator<Map.Entry<String, Object>> iterator = after.entrySet().iterator();
//        while(iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if(!columns.contains(next.getKey())){
//                iterator.remove();
//            }
//        }
        after.entrySet().removeIf(next->!columns.contains(next.getKey()));
    }


    //处理广播流
    //value格式：{"db":,"tn":,"before":{},"after":{},"type":}
    @Override
    public void processBroadcastElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        //1.获取并广播数据
        JSONObject jsonObject = JSON.parseObject(value);
//        System.out.println(value);
        String after = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);
        System.out.println(tableProcess.getSink_type().toString());
        //2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSink_type())){
            System.out.println(tableProcess.getSink_type().toString());
            checkTable(tableProcess.getSink_table(),
                    tableProcess.getSink_columns(),
                    tableProcess.getSink_pk(),
                    tableProcess.getSink_extend());
        }
        //3.写入状态,广播出去
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSource_table()+"-"+tableProcess.getOperate_type();
        broadcastState.put(key, tableProcess);
    }
    //hbase建表:create table if not exists table_name(id varchar primary key) xxx; poenix建表方法
//    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
//        PreparedStatement preparedStatement = null;
//        //字段判空
//        if(sinkPk == null){
//            sinkPk = "id";
//        }
//        if(sinkExtend == null){
//            sinkExtend = "";
//        }
//
//        StringBuffer createTabelSQL = new StringBuffer("create table if not exists ")
//                .append(GmallConfig.HBASE_SCHEMA)
//                .append(".")
//                .append(sinkTable)
//                .append("(");
//        String[] fields = sinkColumns.split(",");
//        for (int i = 0; i < fields.length; i++) {
//            String field = fields[i];
//            //主键判断
//            if(sinkTable.equals(field)){
//                createTabelSQL.append(field).append(" varchar primary key");
//            }else{
//                createTabelSQL.append(field).append(" varchar");
//            }
//            //判断是否为最后一个字段
//            if (i < fields.length-1){
//                createTabelSQL.append(",");
//            }
//        }
//        createTabelSQL.append(")").append(sinkExtend);
//        //预编译SQL
//        try {
//            preparedStatement = connection.prepareStatement(createTabelSQL.toString());
//            preparedStatement.execute();
//        } catch (SQLException e) {
//            System.out.println(createTabelSQL.toString());
//            throw new RuntimeException("phoenix表"+sinkTable+"建表失败");
//        }finally {
//            if(preparedStatement != null){
//                try {
//                    preparedStatement.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
    //hbase建表
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend){
        System.out.println("开始创建Hbase表格>>>>"+sinkTable);
        //字段判空
        if(sinkPk == null){
            sinkPk = "id";
        }
        if(sinkExtend == null){
            sinkExtend = "";
        }
        try {
            HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
            TableName tableName = TableName.valueOf("dzy:"+sinkTable);
            if(admin.tableExists(tableName)){
                System.out.println("Hbase表格》》》》》"+tableName.toString()+"》》》》》》已经创建");
            }else {
                //创建表格
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                hTableDescriptor.addFamily(new HColumnDescriptor("base"));
                admin.createTable(hTableDescriptor);
                System.out.println("Hbase表格"+tableName.toString()+"创建成功");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
