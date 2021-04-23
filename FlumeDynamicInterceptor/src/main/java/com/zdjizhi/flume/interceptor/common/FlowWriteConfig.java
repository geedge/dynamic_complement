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

//    public static final String SEGMENTATION = ",";

    /**
     * System
     */
//    public static final Integer DATA_CENTER_ID_NUM = FlowWriteConfigurations.getIntProperty(0, "data.center.id.num");
//    public static final String CHECK_IP_SCOPE = FlowWriteConfigurations.getStringProperty(0, "check.ip.scope");

    /**
     * kafka
     */
//    public static final String KAFKA_TOPIC = FlowWriteConfigurations.getStringProperty(0, "kafka.topic");

    /**
     * zookeeper
     */
//    public static final String ZOOKEEPER_SERVERS = FlowWriteConfigurations.getStringProperty(0, "zookeeper.servers");
    /***
     * Redis
     */
//    public static final String REDIS_IP = "redis.ip";
//    public static final String REDIS_PORT = "redis.port";
//    public static final String REDIS_TIMEOUT = "redis.timeout";
//    public static final String REDIS_POOL_MAXACTIVE = "redis.pool.maxActive";
//    public static final String REDIS_POOL_MAXIDLE = "redis.pool.maxIdle";
//    public static final String REDIS_POOL_MAXWAIT = "redis.pool.maxWait";
//    public static final String REDIS_POOL_TESTONBORROW = "redis.pool.testOnBorrow";
//    public static final String REDIS_POOL_TESTONRETURN = "redis.pool.testOnReturn";

    /**
     * flume使用配置
     */
    public static final String SCHEMA_HTTP = FlowWriteConfigurations.getStringProperty(0, "schema.http");
    public static final String IP_LIBRARY = FlowWriteConfigurations.getStringProperty(0, "ip.library");
    public static final String HBASE_ZOOKEEPER_SERVERS = FlowWriteConfigurations.getStringProperty(0, "hbase.zookeeper.servers");
    public static final String HBASE_TABLE_NAME = FlowWriteConfigurations.getStringProperty(0, "hbase.table.name");

    public static final String MAIL_DEFAULT_CHARSET = FlowWriteConfigurations.getStringProperty(0, "mail.default.charset");




}