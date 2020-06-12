package com.zdjizhi.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.zdjizhi.flume.interceptor.common.SubscriberConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;


/**
 * @author qidaijie
 */
public class FlumeSubscriberApp implements Interceptor {
    private static Logger logger = Logger.getLogger(FlumeSubscriberApp.class);

    private static Map<String, String> subIdMap;
    private List<Put> putList;
    private static Connection connection;
    private String hbaseZookeeperIp;
    private String hbaseTableName;
    private int updateHBaseTime;

    public FlumeSubscriberApp(String hbaseZookeeperIp, String hbaseTableName, int updateHBaseTime) {
        this.hbaseZookeeperIp = hbaseZookeeperIp;
        this.hbaseTableName = hbaseTableName;
        this.updateHBaseTime = updateHBaseTime;
    }


    @Override
    public void initialize() {
        subIdMap = new HashMap<>(256);
        putList = new ArrayList<>();

        // 管理HBase的配置信息
        Configuration configuration = HBaseConfiguration.create();
        // 设置zookeeper节点
        configuration.set("hbase.zookeeper.quorum", hbaseZookeeperIp);
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("hbase.client.retries.number", "3");
        configuration.set("hbase.bulkload.retries.number", "3");
        configuration.set("zookeeper.recovery.retry", "3");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            logger.error("用户名信息写入HBase程序，连接HBase异常");
            e.printStackTrace();
        }

        getAll(hbaseTableName);

        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    insertData(putList, hbaseTableName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 0, updateHBaseTime * 1000);
    }


    @Override
    public Event intercept(Event event) {
        String message = null;
        try {
            message = new String(event.getBody(), "utf-8");
        } catch (UnsupportedEncodingException e) {
            message = new String(event.getBody());
        }
        try {
            if (StringUtils.isNotBlank(message)) {
                message = dealCommonMessage(message);
                event.setBody(message.getBytes());
                return event;
            }
        } catch (Exception e) {
            logger.error("FlumeSubscriberApp intercept(Event event) method is error===>{" + e + "}<===");
            e.printStackTrace();
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        List resultList = new ArrayList();
        for (Event event : list) {
            Event r = intercept(event);
            if (r != null) {
                resultList.add(r);
            }
        }
        return resultList;
    }

    @Override
    public void close() {
        logger.warn("FlumeSubscriberApp is closed.");
    }

    /**
     * 解析日志，并补全
     * 补domain,补subscriber_id
     *
     * @param message Security原始日志
     * @return 补全后的日志
     * <p>
     */
    private String dealCommonMessage(String message) {
        JSONObject jsonObject = JSONObject.parseObject(message);
        if (jsonObject.containsKey(SubscriberConfig.PACKET_TYPE) && jsonObject.containsKey(SubscriberConfig.STATUS_TYPE)) {
            if (SubscriberConfig.ACCOUNTING_REQUEST == jsonObject.getInteger(SubscriberConfig.PACKET_TYPE)
                    && SubscriberConfig.START_BILLING == jsonObject.getInteger(SubscriberConfig.STATUS_TYPE)) {
                String framedIp = jsonObject.getString("radius_framed_ip");
                String account = jsonObject.getString("radius_account");
                dataValidation(framedIp, account, putList);
            }
            if (putList.size() == SubscriberConfig.LIST_SIZE_MAX) {
                insertData(putList, hbaseTableName);
            }

        }
        return message;
    }


    /**
     * 获取所有的 key value
     */
    private static void getAll(String tableNmae) {
        try {
            Table table = connection.getTable(TableName.valueOf("sub:" + tableNmae));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    subIdMap.put(Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            scanner.close();
        } catch (IOException e) {
            logger.error("获取HBase所有row key出现异常");
            e.printStackTrace();
        }
    }

    /**
     * 写入数据到HBase
     *
     * @param putList puts list
     */
    private static void insertData(List<Put> putList, String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf("sub:" + tableName));
            table.put(putList);
            putList.clear();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                logger.error("更新数据写入HBase失败");
                e.printStackTrace();
            }
        }

    }

    /**
     * 验证数据并与内存中的对比
     *
     * @param ip      framed_ip
     * @param account account
     */
    private static void dataValidation(String ip, String account, List<Put> putList) {
        if (subIdMap.containsKey(ip)) {
            if (!subIdMap.get(ip).equals(account)) {
                Put put = new Put(ip.getBytes());
                put.addColumn("subscriber_id".getBytes(), "account".getBytes(), account.getBytes());
                putList.add(put);
                subIdMap.put(ip, account);
            }
        } else {
            Put put = new Put(ip.getBytes());
            put.addColumn("subscriber_id".getBytes(), "account".getBytes(), account.getBytes());
            putList.add(put);
            subIdMap.put(ip, account);
        }
    }


    public static class FlumeDynamicAppBuilder implements Interceptor.Builder {
        private String hbaseZookeeperIp;
        private String hbaseTableName;
        private int updateHBaseTime;


        @Override
        public Interceptor build() {
            return new FlumeSubscriberApp(hbaseZookeeperIp, hbaseTableName, updateHBaseTime);
        }

        @Override
        public void configure(Context context) {
            try {
                this.hbaseZookeeperIp = context.getString("hbaseZookeeperIp", "");
                Preconditions.checkNotNull("".equals(hbaseZookeeperIp), "hbaseZookeeperIp must be set!!");
                logger.info("FlumeSubscriberApp Read hbaseZookeeperIp from configuration : " + hbaseZookeeperIp);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("FlumeSubscriberApp hbaseZookeeperIp invalid", e);
            } catch (Exception e) {
                logger.error("FlumeSubscriberApp Get hbaseZookeeperIp is error : " + e);
            }

            try {
                this.hbaseTableName = context.getString("hbaseTableName", "");
                Preconditions.checkNotNull("".equals(hbaseTableName), "hbaseTableName must be set!!");
                logger.info("FlumeSubscriberApp Read hbaseTableName from configuration : " + hbaseTableName);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("FlumeSubscriberApp hbaseTableName invalid", e);
            } catch (Exception e) {
                logger.error("FlumeSubscriberApp Get hbaseTableName is error : " + e);
            }

            try {
                this.updateHBaseTime = context.getInteger("updateHBaseTime", 30);
                Preconditions.checkNotNull("".equals(updateHBaseTime), "updateHBaseTime must be set!!");
                logger.info("FlumeSubscriberApp Read updateHBaseTime from configuration : " + updateHBaseTime);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("FlumeSubscriberApp updateHBaseTime invalid", e);
            } catch (Exception e) {
                logger.error("FlumeSubscriberApp Get updateHBaseTime is error : " + e);
            }

        }

    }

}

