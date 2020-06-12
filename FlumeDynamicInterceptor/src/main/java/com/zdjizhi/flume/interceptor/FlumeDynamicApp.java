package com.zdjizhi.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.zdjizhi.flume.interceptor.common.FlowWriteConfig;
import com.zdjizhi.flume.interceptor.utils.hbase.HBaseUtils;
import com.zdjizhi.flume.interceptor.utils.json.JsonParseUtil;
import com.zdjizhi.utils.Encodes;
import com.zdjizhi.utils.FormatUtils;
import com.zdjizhi.utils.IpLookup;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * @author qidaijie
 */
public class FlumeDynamicApp implements Interceptor {
    private static Logger logger = Logger.getLogger(FlumeDynamicApp.class);

    private static IpLookup ipLookup;
    private static FormatUtils formatUtils;
    private String schemaHttpUrl;
    private String uidZookeeperIp;
    private long dataCenterIdNum;
    private String ipDatPath;
    private String hbaseZookeeperIp;
    private String hbaseTableName;

    private static HashMap<String, Class> map;
    private static Object mapObject;
    private static ArrayList<String[]> jobList;

    @Override
    public void initialize() {
        map = JsonParseUtil.getMapFromhttp(schemaHttpUrl);
        mapObject = JsonParseUtil.generateObject(map);
        jobList = JsonParseUtil.getJobListFromHttp(schemaHttpUrl);
        //载入工具类
        formatUtils = new FormatUtils.Builder(false).build();
        //载入定位库
        ipLookup = new IpLookup.Builder(false)
                /**
                 * v1.0.2-com.zdjizhi.galaxy
                 */
//                .loadDataFileV4(ipDatPath + "Kazakhstan.mmdb")
//                .loadDataFileV6(ipDatPath + "Kazakhstan.mmdb")
//                .loadAsnDataFileV4(ipDatPath + "asn_v4.mmdb")
//                .loadAsnDataFileV6(ipDatPath + "asn_v6.mmdb")

                /**
                 * v1.0.3-com.zdjizhi.galaxy
                 */
                .loadDataFile(ipDatPath + "all_ip_info_v4.mmdb")
                .loadDataFilePrivateV4(ipDatPath + "ip_private.mmdb")
                .loadDataFilePrivateV6(ipDatPath + "ip_private.mmdb")
                .loadAsnDataFile(ipDatPath + "asn_v4.mmdb")
                .loadAsnDataFileV6(ipDatPath + "asn_v6.mmdb")
                .build();
    }

    public FlumeDynamicApp(String schemaHttpUrl, String zookeeperIp, long dataCenterIdNum, String ipDatPath, String hbaseZookeeperIp, String hbaseTableName) {
        this.schemaHttpUrl = schemaHttpUrl;
        this.uidZookeeperIp = zookeeperIp;
        this.dataCenterIdNum = dataCenterIdNum;
        this.ipDatPath = ipDatPath;
        this.hbaseZookeeperIp = hbaseZookeeperIp;
        this.hbaseTableName = hbaseTableName;
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
            logger.error("FlumeDynamicApp intercept(Event event) method is error===>{" + e + "}<===");
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
        logger.warn("FlumeDynamicApp is closed.");
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
        Object object = JSONObject.parseObject(message, mapObject.getClass());

        try {
            for (String[] strings : jobList) {
                Object name = JsonParseUtil.getValue(object, strings[0]);
                String appendToKeyName = strings[1];
                String functionName = strings[2];
                Object param = null;
                if (strings[3] != null) {
                    param = JsonParseUtil.getValue(object, strings[3]);
                }

                switch (functionName) {
                    case "current_timestamp":
                        JsonParseUtil.setValue(object, appendToKeyName, getCurrentTime());
                        break;
                    case "snowflake_id":
                        JsonParseUtil.setValue(object, appendToKeyName, formatUtils.getSnowflakeId(uidZookeeperIp, dataCenterIdNum));
                        break;
                    case "geo_ip_detail":
                        if (name != null && JsonParseUtil.getValue(object, appendToKeyName) == null) {
                            JsonParseUtil.setValue(object, appendToKeyName, getGeoIpDetail(name.toString()));
                        }
                        break;
                    case "geo_asn":
                        if (name != null && JsonParseUtil.getValue(object, appendToKeyName) == null) {
                            JsonParseUtil.setValue(object, appendToKeyName, getGeoAsn(name.toString()));
                        }
                        break;
                    case "get_value":
                        if (name != null) {
                            JsonParseUtil.setValue(object, appendToKeyName, name.toString());
                        }
                        break;
                    case "radius_match":
                        if (name != null) {
                            JsonParseUtil.setValue(object, appendToKeyName, radiusMatch(name.toString(), hbaseZookeeperIp, hbaseTableName));
                        }
                        break;
                    case "geo_ip_country":
                        if (name != null && JsonParseUtil.getValue(object, appendToKeyName) == null) {
                            JsonParseUtil.setValue(object, appendToKeyName, getGeoIpCountry(name.toString()));
                        }
                        break;
                    case "decode_of_base64":
                        if (name != null) {
                            if (param != null) {
                                JsonParseUtil.setValue(object, appendToKeyName, Encodes.decodeBase64String(name.toString(), param.toString()));
                            } else {
                                JsonParseUtil.setValue(object, appendToKeyName, Encodes.decodeBase64String(name.toString(), FlowWriteConfig.MAIL_DEFAULT_CHARSET));
                            }
                        }
                        break;
                    case "sub_domain":
                        if (name != null) {
                            Object appendTo = JsonParseUtil.getValue(object, appendToKeyName);
                            if (appendTo == null || StringUtil.isBlank(appendTo.toString())) {
                                JsonParseUtil.setValue(object, appendToKeyName, replaceGetTopDomain(FormatUtils.getTopPrivateDomain(name.toString())));
                            }
                        }
                        break;
                    default:
                }
            }

            return JSONObject.toJSONString(object);

        } catch (Exception e) {
            logger.error("FlumeDynamicApp dealCommonMessage is error===>{" + e + "}<===");
            e.printStackTrace();
//            return "";
            return message;//返回原数据
        }
    }

    /**
     * 有host根据host获取域名,有sni通过sni获取域名
     * 这里是直接根据传入的获取域名
     *
     * @param url
     * @return 顶级域名
     */
    private String replaceGetTopDomain(String url) {
        return url.replaceAll("InternetDomainName\\{name=", "").replaceAll("\\}", "");
    }

    /**
     * 生成当前时间戳的操作
     */
    private int getCurrentTime() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    /**
     * 根据clientIp获取location信息
     *
     * @param ip
     * @return
     */
    private String getGeoIpDetail(String ip) {
        return ipLookup.cityLookupDetail(ip);
    }

    /**
     * 根据ip获取asn信息
     *
     * @param ip
     * @return
     */
    private String getGeoAsn(String ip) {
//        return ipLookup.asnLookup(ip, true);//v1.0.2-com.zdjizhi.galaxy
        return ipLookup.asnLookup(ip);//v1.0.3-com.zdjizhi.galaxy
    }

    /**
     * 根据ip获取country信息
     *
     * @param ip
     * @return
     */
    private String getGeoIpCountry(String ip) {
        return ipLookup.countryLookup(ip);
    }

    /**
     * radius借助hbase补齐
     *
     * @param ip
     * @return
     */
    private static String radiusMatch(String ip, String hbaseZookeeper, String hbaseTable) {
        return HBaseUtils.getAccount(ip, hbaseZookeeper, hbaseTable);
    }

    public static class FlumeDynamicAppBuilder implements Interceptor.Builder {
        private String schemaHttpUrl;
        private String uidZookeeperIp;
        private long dataCenterIdNum;
        private String ipDatPath;
        private String hbaseZookeeperIp;
        private String hbaseTableName;


        @Override
        public Interceptor build() {
            return new FlumeDynamicApp(this.schemaHttpUrl,
                    this.uidZookeeperIp, this.dataCenterIdNum,
                    this.ipDatPath,
                    this.hbaseZookeeperIp, this.hbaseTableName);
        }

        @Override
        public void configure(Context context) {
            try {
                this.schemaHttpUrl = context.getString("schemaHttpUrl", "");
                Preconditions.checkNotNull("".equals(schemaHttpUrl), "schemaHttpUrl must be set!!");
                logger.info("FlumeDynamicApp Read schemaHttpUrl from configuration : " + schemaHttpUrl);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("FlumeDynamicApp schemaHttpUrl invalid", e);
            } catch (Exception e) {
                logger.error("FlumeDynamicApp Get schemaHttpUrl is error : " + e);
            }

            try {
                this.uidZookeeperIp = context.getString("uidZookeeperIp", "");
                Preconditions.checkNotNull("".equals(uidZookeeperIp), "uidZookeeperIp must be set!!");
                logger.info("FlumeDynamicApp Read uidZookeeperIp from configuration : " + uidZookeeperIp);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("FlumeDynamicApp uidZookeeperIp invalid", e);
            } catch (Exception e) {
                logger.error("FlumeDynamicApp Get uidZookeeperIp is error : " + e);
            }

            try {
                this.dataCenterIdNum = context.getInteger("dataCenterIdNum", RandomUtils.nextInt(0, 16));//0-16,不包含16,即只有0-15
                if (dataCenterIdNum < 0L || dataCenterIdNum > 15L) {
                    logger.error("\ndataCenterIdNum is out of effective range,dataCenterIdNum must be greater than 0(include 0) and less than 16(exclusive 16),but there is " + dataCenterIdNum);
                    this.dataCenterIdNum = RandomUtils.nextInt(0, 16);
                    logger.error("\nso system will auto get a random num between 0 and 15,so this random dataCenterIdNum is " + dataCenterIdNum);
                } else {
                    logger.info("FlumeDynamicApp Read dataCenterIdNum from configuration : " + dataCenterIdNum);
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("FlumeDynamicApp dataCenterIdNum invalid", e);
            } catch (Exception e) {
                logger.error("FlumeDynamicApp Get dataCenterIdNum is error : " + e);
            }

            try {
                this.ipDatPath = context.getString("ipDatPath", "");
                Preconditions.checkNotNull("".equals(ipDatPath), "ipDatPath must be set!!");
                if ((!"".equals(ipDatPath)) && (!ipDatPath.endsWith("/"))) {
                    this.ipDatPath = ipDatPath + "/";
                }
                logger.info("FlumeDynamicApp Read ipDatPath from configuration : " + ipDatPath);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("FlumeDynamicApp ipDatPath invalid", e);
            } catch (Exception e) {
                logger.error("FlumeDynamicApp Get ipDatPath is error : " + e);
            }

            try {
                this.hbaseZookeeperIp = context.getString("hbaseZookeeperIp", "");
                Preconditions.checkNotNull("".equals(hbaseZookeeperIp), "hbaseZookeeperIp must be set!!");
                logger.info("FlumeDynamicApp Read hbaseZookeeperIp from configuration : " + hbaseZookeeperIp);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("FlumeDynamicApp hbaseZookeeperIp invalid", e);
            } catch (Exception e) {
                logger.error("FlumeDynamicApp Get hbaseZookeeperIp is error : " + e);
            }

            try {
                this.hbaseTableName = context.getString("hbaseTableName", "");
                Preconditions.checkNotNull("".equals(hbaseTableName), "hbaseTableName must be set!!");
                logger.info("FlumeDynamicApp Read hbaseTableName from configuration : " + hbaseTableName);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("FlumeDynamicApp hbaseTableName invalid", e);
            } catch (Exception e) {
                logger.error("FlumeDynamicApp Get hbaseTableName is error : " + e);
            }

        }
    }
}

