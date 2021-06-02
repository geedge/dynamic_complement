package com.zdjizhi.flume.interceptor.common;


import com.zdjizhi.flume.interceptor.utils.system.FlowWriteConfigurations;

/**
 * @author Administrator
 */
public class FlowWriteConfig {
    public static final String VISIBILITY = "disabled";
    public static final int IF_PARAM_LENGTH = 3;
    public static final String FORMAT_SPLITTER = ",";
    public static final String IS_JSON_KEY_TAG = "$.";
    public static final String IF_CONDITION_SPLITTER = "=";

    /**
     * flume使用配置
     */
    public static final String SCHEMA_HTTP = FlowWriteConfigurations.getStringProperty(0, "schema.http");
    public static final String IP_LIBRARY = FlowWriteConfigurations.getStringProperty(0, "ip.library");
    public static final String HBASE_ZOOKEEPER_SERVERS = FlowWriteConfigurations.getStringProperty(0, "hbase.zookeeper.servers");
    public static final String HBASE_TABLE_NAME = FlowWriteConfigurations.getStringProperty(0, "hbase.table.name");

    public static final String MAIL_DEFAULT_CHARSET = FlowWriteConfigurations.getStringProperty(0, "mail.default.charset");




}