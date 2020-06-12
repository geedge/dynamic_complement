package com.zdjizhi.flume.interceptor.common;


/**
 * @author Administrator
 */
public class SubscriberConfig {
    /**
     * 最多存在多少数据即写入hbase
     */
    public static final int LIST_SIZE_MAX = 5000;
    /**
     * 4- Accounting-Request(账户授权)
     */
    public static final int ACCOUNTING_REQUEST = 4;
    /**
     * radius_packet_type
     */
    public static final String PACKET_TYPE = "radius_packet_type";
    /**
     * 1、开始计费
     */
    public static final int START_BILLING = 1;
    /**
     * radius_acct_status_type
     */
    public static final String STATUS_TYPE = "radius_acct_status_type";

    /**
     * flume使用配置
     */
    public static final String HBASE_ZOOKEEPER_SERVERS = SubscriberConfigurations.getStringProperty(0, "hbase.zookeeper.servers");
    public static final String HBASE_TABLE_NAME = SubscriberConfigurations.getStringProperty(0, "hbase.table.name");
}