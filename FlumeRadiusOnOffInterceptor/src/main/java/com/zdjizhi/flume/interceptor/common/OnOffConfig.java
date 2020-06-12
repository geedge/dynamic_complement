package com.zdjizhi.flume.interceptor.common;


/**
 * @author Administrator
 */
public class OnOffConfig {
    /**
     * 4- Accounting-Request(账户授权)
     */
    public static final int ACCOUNTING_REQUEST = 4;
    /**
     * 1、开始计费
     */
    public static final int START_BILLING = 1;
    /**
     * 2、停止计费
     */
    public static final int STOP_BILLING = 2;

    /**
     * 计费请求报文类型
     */
    public static final String RADIUS_ACCT_STATUS_TYPE = "radius_acct_status_type";
    /**
     * 报文类型
     */
    public static final String RADIUS_PACKET_TYPE = "radius_packet_type";

    /**
     * 发送计费请求报文时间戳
     */
    public static final String RADIUS_EVENT_TIMESTAMP = "radius_event_timestamp";

    /**
     * 一个用户多个计费ID关联属性
     */
    public static final String RADIUS_MULTI_SESSION_ID = "radius_acct_multi_session_id";

    /**
     * 用户的在线时长，以秒为单位
     */
    public static final String RADIUS_SESSION_TIME = "radius_acct_session_time";

    /**
     * flume使用配置
     */
    public static final String HBASE_ZOOKEEPER_SERVERS = OnOffConfigurations.getStringProperty(0, "hbase.zookeeper.servers");
    public static final String HBASE_TABLE_NAME = OnOffConfigurations.getStringProperty(0, "hbase.table.name");
}