package com.zdjizhi.flume.interceptor.utils.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;


/**
 * HBase 工具类
 *
 * @author qidaijie
 */

public class HBaseUtils {
    private final static Logger logger = Logger.getLogger(HBaseUtils.class);
    private static Map<String, String> subIdMap = new HashMap<>(128);
    private static Connection connection;
    private static Long time;

    private static String zookeeperIp;
    private static String hbaseTable;

    private static HBaseUtils hBaseUtils;

    private static void getHbaseInstance(String zookeeperServer, String hbaseTableName) {
        hBaseUtils = new HBaseUtils(zookeeperServer, hbaseTableName);
    }

    /**
     * 构造函数-新
     */
    private HBaseUtils(String zookeeperServer, String hbaseTableName) {
        zookeeperIp = zookeeperServer;
        hbaseTable = hbaseTableName;
        //获取连接
        getHbaseConn();
        //拉取所有
        getAll();
        //定时更新
        updateHabaseCache();
    }

    private static void getHbaseConn() {
        try {
            // 管理Hbase的配置信息
            Configuration configuration = HBaseConfiguration.create();
            // 设置zookeeper节点
            configuration.set("hbase.zookeeper.quorum", zookeeperIp);
            configuration.set("hbase.client.retries.number", "3");
            configuration.set("hbase.bulkload.retries.number", "3");
            configuration.set("zookeeper.recovery.retry", "3");
            connection = ConnectionFactory.createConnection(configuration);
            time = System.currentTimeMillis();
            logger.warn("HBaseUtils get HBase connection,now to getAll().");
//            getAll();
        } catch (IOException ioe) {
            logger.error("HBaseUtils getHbaseConn() IOException===>{" + ioe + "}<===");
            ioe.printStackTrace();
        } catch (Exception e) {
            logger.error("HBaseUtils getHbaseConn() Exception===>{" + e + "}<===");
            e.printStackTrace();
        }
    }

    /**
     * 更新变量
     */
    private static void change() {
        long nowTime = System.currentTimeMillis();
        timestampsFilter(time - 1000, nowTime + 500);
    }

    /**
     * 获取变更内容
     *
     * @param startTime 开始时间
     * @param endTime   结束时间
     */
    private static void timestampsFilter(Long startTime, Long endTime) {
        Long begin = System.currentTimeMillis();
        Table table = null;
        ResultScanner scanner = null;
        Scan scan2 = new Scan();
        try {
            table = connection.getTable(TableName.valueOf("sub:" + hbaseTable));
//            table = connection.getTable(TableName.valueOf("sub:" + FlowWriteConfig.HBASE_TABLE_NAME));
            scan2.setTimeRange(startTime, endTime);
            scanner = table.getScanner(scan2);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    String key = Bytes.toString(CellUtil.cloneRow(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    if (subIdMap.containsKey(key)) {
                        if (!value.equals(subIdMap.get(key))) {
                            subIdMap.put(key, value);
                        }
                    } else {
                        subIdMap.put(key, value);
                    }
                }
            }
            Long end = System.currentTimeMillis();
            logger.warn("HBaseUtils Now subIdMap.keySet().size() is: " + subIdMap.keySet().size());
            logger.warn("HBaseUtils Update cache timeConsuming is: " + (end - begin) + ",BeginTime: " + begin + ",EndTime: " + end);
            time = endTime;
        } catch (IOException ioe) {
            logger.error("HBaseUtils timestampsFilter is IOException===>{" + ioe + "}<===");
            ioe.printStackTrace();
        } catch (Exception e) {
            logger.error("HBaseUtils timestampsFilter is Exception===>{" + e + "}<===");
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 获取所有的 key value
     */
    private static void getAll() {
        long begin = System.currentTimeMillis();
        try {
            Table table = connection.getTable(TableName.valueOf("sub:" + hbaseTable));
//            Table table = connection.getTable(TableName.valueOf("sub:" + FlowWriteConfig.HBASE_TABLE_NAME));
            Scan scan2 = new Scan();
            ResultScanner scanner = table.getScanner(scan2);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    subIdMap.put(Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            logger.warn("HBaseUtils Get fullAmount List size->subIdMap.size(): " + subIdMap.size());
            logger.warn("HBaseUtils Get fullAmount List size->subIdMap.size() timeConsuming is: " + (System.currentTimeMillis() - begin));
            scanner.close();
        } catch (IOException ioe) {
            logger.error("HBaseUtils getAll() is IOException===>{" + ioe + "}<===");
            ioe.printStackTrace();
        } catch (Exception e) {
            logger.error("HBaseUtils getAll() is Exception===>{" + e + "}<===");
            e.printStackTrace();
        }
    }

    /**
     * 获取 account
     *
     * @param clientIp client_ip
     * @return account
     */
    public static String getAccount(String clientIp, String hbaseZookeeper, String hbaseTable) {
        if (hBaseUtils == null) {
            getHbaseInstance(hbaseZookeeper, hbaseTable);
        }
        return subIdMap.get(clientIp);
    }

    /**
     * 验证定时器,每隔一段时间验证一次-验证获取新的Cookie
     */
    private void updateHabaseCache() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    change();
                } catch (Exception e) {
                    logger.error("HBaseUtils update hbaseCache is error===>{" + e + "}<===");
                    e.printStackTrace();
                }
            }
        }, 1, 1000 * 60);//起始1ms,以后每隔60s
    }

}
