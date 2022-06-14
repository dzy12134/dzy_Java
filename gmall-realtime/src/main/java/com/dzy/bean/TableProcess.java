package com.dzy.bean;

public class TableProcess {
    //动态分流 Sink 常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";
    //来源表
    String source_table;
    //操作类型 insert,update,delete
    String operate_type;
    //输出类型 hbase kafka
    String sink_type;
    //输出表(主题)
    String sink_table;
    //输出字段
    String sink_columns;
    //主键字段
    String sink_pk;
    //建表扩展
    String sink_extend;

    public static String getSinkTypeHbase() {
        return SINK_TYPE_HBASE;
    }

    public static String getSinkTypeKafka() {
        return SINK_TYPE_KAFKA;
    }

    public static String getSinkTypeCk() {
        return SINK_TYPE_CK;
    }

    public String getSource_table() {
        return source_table;
    }

    public void setSource_table(String source_table) {
        this.source_table = source_table;
    }

    public String getOperate_type() {
        return operate_type;
    }

    public void setOperate_type(String operate_type) {
        this.operate_type = operate_type;
    }

    public String getSink_type() {
        return sink_type;
    }

    public void setSink_type(String sink_type) {
        this.sink_type = sink_type;
    }

    public String getSink_table() {
        return sink_table;
    }

    public void setSink_table(String sink_table) {
        this.sink_table = sink_table;
    }

    public String getSink_columns() {
        return sink_columns;
    }

    public void setSink_columns(String sink_columns) {
        this.sink_columns = sink_columns;
    }

    public String getSink_pk() {
        return sink_pk;
    }

    public void setSink_pk(String sink_pk) {
        this.sink_pk = sink_pk;
    }

    public String getSink_extend() {
        return sink_extend;
    }

    public void setSink_extend(String sink_extend) {
        this.sink_extend = sink_extend;
    }
}
