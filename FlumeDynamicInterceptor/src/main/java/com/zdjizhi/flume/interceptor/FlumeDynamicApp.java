package com.zdjizhi.flume.interceptor;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.zdjizhi.flume.interceptor.common.FlowWriteConfig;
import com.zdjizhi.flume.interceptor.utils.app.AppUtils;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author qidaijie
 */
public class FlumeDynamicApp implements Interceptor {
    private static final Log logger = LogFactory.get();
    private static final Pattern PATTERN = Pattern.compile("[0-9]*");

    private static IpLookup ipLookup;
    private static FormatUtils formatUtils;
    private String schemaHttpUrl;
    private String appIdHttpUrl;
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
                .loadDataFileV4(ipDatPath + "ip_v4.mmdb")
                .loadDataFileV6(ipDatPath + "ip_v6.mmdb")
                .loadDataFilePrivateV4(ipDatPath + "ip_private_v4.mmdb")
                .loadDataFilePrivateV6(ipDatPath + "ip_private_v6.mmdb")
                .loadAsnDataFile(ipDatPath + "asn_v4.mmdb")
                .loadAsnDataFileV6(ipDatPath + "asn_v6.mmdb")
                .build();
    }

    public FlumeDynamicApp(String schemaHttpUrl, String appIdHttpUrl, String zookeeperIp, long dataCenterIdNum, String ipDatPath, String hbaseZookeeperIp, String hbaseTableName) {
        this.schemaHttpUrl = schemaHttpUrl;
        this.appIdHttpUrl = appIdHttpUrl;
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
                Object appendTo = JsonParseUtil.getValue(object, appendToKeyName);

                String param = strings[3];

                switch (functionName) {
                    case "current_timestamp":
                        if ((long) appendTo == 0L) {
                            JsonParseUtil.setValue(object, appendToKeyName, getCurrentTime());
                        }
                        break;
                    case "snowflake_id":
                        JsonParseUtil.setValue(object, appendToKeyName, formatUtils.getSnowflakeId(uidZookeeperIp, dataCenterIdNum));
                        break;
                    case "set_value":
                        if (name != null && param != null) {
                            JsonParseUtil.setValue(object, appendToKeyName, setValue(param));
                        }
                        break;
                    case "flattenSpec":
                        if (name != null && StringUtil.isNotBlank(param)) {
                            JsonParseUtil.setValue(object, appendToKeyName, flattenSpec(name.toString(), isJsonValue(object, param)));
                        }
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
                    case "if":
                        if (param != null) {
                            JsonParseUtil.setValue(object, appendToKeyName, condition(object, param));
                        }
                        break;
                    case "get_value":
                        if (name != null) {
                            JsonParseUtil.setValue(object, appendToKeyName, name);
                        }
                        break;
                    case "radius_match":
                        if (name != null) {
                            JsonParseUtil.setValue(object, appendToKeyName, radiusMatch(name.toString(), hbaseZookeeperIp, hbaseTableName));
                        }
                        break;
                    case "app_match":
                        if (name != null && appendTo == null) {
                            JsonParseUtil.setValue(object, appendToKeyName, appMatch(appIdHttpUrl, name.toString()));
                        }
                        break;
                    case "geo_ip_country":
                        if (name != null && JsonParseUtil.getValue(object, appendToKeyName) == null) {
                            JsonParseUtil.setValue(object, appendToKeyName, getGeoIpCountry(name.toString()));
                        }
                        break;
                    case "decode_of_base64":
                        if (name != null) {
                            JsonParseUtil.setValue(object, appendToKeyName, decodeBase64(name.toString(), isJsonValue(object, param)));
                        }
                        break;
                    case "sub_domain":
                        if (appendTo == null && name != null) {
                            JsonParseUtil.setValue(object, appendToKeyName, replaceGetTopDomain(name.toString()));
                        }
                        break;
                    default:
                }
            }

            return JSONObject.toJSONString(object);

        } catch (Exception e) {
            logger.error("FlumeDynamicApp dealCommonMessage is error===>", e);
            //返回原数据
            return message;
        }
    }

    /**
     * 有host根据host获取域名,有sni通过sni获取域名
     * 这里是直接根据传入的获取域名
     *
     * @param url
     * @return 顶级域名
     */
    private static String replaceGetTopDomain(String url) {
        try {
            return FormatUtils.getTopPrivateDomain(url).replaceAll("InternetDomainName\\{name=", "").replaceAll("\\}", "");
        } catch (StringIndexOutOfBoundsException outException) {
            logger.error("解析顶级域名异常,异常域名:" + url, outException);
            return "";
        }
    }

    /**
     * 生成当前时间戳的操作
     */
    private static long getCurrentTime() {
        return (System.currentTimeMillis() / 1000);
    }

    /**
     * 根据clientIp获取location信息
     *
     * @param ip
     * @return
     */
    private static String getGeoIpDetail(String ip) {
        return ipLookup.cityLookupDetail(ip);
    }

    /**
     * 根据ip获取asn信息
     *
     * @param ip
     * @return
     */
    private static String getGeoAsn(String ip) {
//        return ipLookup.asnLookup(ip, true);//v1.0.2-com.zdjizhi.galaxy
        return ipLookup.asnLookup(ip);//v1.0.3-com.zdjizhi.galaxy
    }

    /**
     * 根据ip获取country信息
     *
     * @param ip
     * @return
     */
    private static String getGeoIpCountry(String ip) {
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

    /**
     * appId与缓存中对应关系补全appName
     *
     * @param appIds id
     * @return appName
     */
    private static String appMatch(String appIdHttpUrl, String appIds) {
        String appId = appIds.split(FlowWriteConfig.FORMAT_SPLITTER)[0];
        String appName = AppUtils.getAppName(appIdHttpUrl, Integer.parseInt(appId));
        if (StringUtil.isBlank(appName)) {
            logger.warn("AppMap get appName is null, ID is :{}", appId);
        }
        return appName;
    }

    /**
     * 根据编码解码base64
     *
     * @param message base64
     * @param charset 编码
     * @return 解码字符串
     */
    private static String decodeBase64(String message, Object charset) {
        String result = "";
        try {
            if (StringUtil.isNotBlank(message)) {
                if (charset != null) {
                    result = Encodes.decodeBase64String(message, charset.toString());
                } else {
                    result = Encodes.decodeBase64String(message, FlowWriteConfig.MAIL_DEFAULT_CHARSET);
                }
            }
        } catch (Exception e) {
            logger.error("解析 Base64 异常,异常信息:" + e);
        }
        return result;
    }

    /**
     * 根据表达式解析json
     *
     * @param message json
     * @param expr    解析表达式
     * @return 解析结果
     */
    private static String flattenSpec(String message, String expr) {
        String flattenResult = "";
        try {
            ArrayList<String> read = JsonPath.parse(message).read(expr);
            flattenResult = read.get(0);
        } catch (ClassCastException | InvalidPathException e) {
            logger.error("设备标签解析异常，[ " + expr + " ]解析表达式错误", e);
        }
        return flattenResult;
    }

    /**
     * 判断是否为日志字段,是则返回对应value，否则返回原始字符串
     *
     * @param object 内存实体类
     * @param param  字段名/普通字符串
     * @return JSON.Value or String
     */
    private static String isJsonValue(Object object, String param) {
        if (param.contains(FlowWriteConfig.IS_JSON_KEY_TAG)) {
            Object value = JsonParseUtil.getValue(object, param.substring(2));
            if (value != null) {
                return value.toString();
            } else {
                return "";
            }
        } else {
            return param;
        }
    }

    /**
     * IF函数实现，解析日志构建三目运算
     *
     * @param object  内存实体类
     * @param ifParam 字段名/普通字符串
     * @return resultA or resultB or ""
     */
    private static Object condition(Object object, String ifParam) {
        try {
            String[] split = ifParam.split(FlowWriteConfig.FORMAT_SPLITTER);
            String[] norms = split[0].split(FlowWriteConfig.IF_CONDITION_SPLITTER);
            String direction = isJsonValue(object, norms[0]);
            if (StringUtil.isNotBlank(direction)) {
                if (split.length == FlowWriteConfig.IF_PARAM_LENGTH) {
                    String resultA = isJsonValue(object, split[1]);
                    String resultB = isJsonValue(object, split[2]);
                    String result = (Integer.parseInt(direction) == Integer.parseInt(norms[1])) ? resultA : resultB;
                    Matcher isNum = PATTERN.matcher(result);
                    if (isNum.matches()) {
                        return Long.parseLong(result);
                    } else {
                        return result;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("IF 函数执行异常,异常信息:" + e);
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 设置固定值函数 若为数字则转为long返回
     *
     * @param param 默认值
     * @return 返回数字或字符串
     */
    static Object setValue(String param) {
        try {
            Matcher isNum = PATTERN.matcher(param);
            if (isNum.matches()) {
                return Long.parseLong(param);
            } else {
                return param;
            }
        } catch (Exception e) {
            logger.error("SetValue 函数异常，异常信息：" + e);
            e.printStackTrace();
        }
        return null;
    }

    public static class FlumeDynamicAppBuilder implements Interceptor.Builder {
        private String schemaHttpUrl;
        private String appIdHttpUrl;
        private String uidZookeeperIp;
        private long dataCenterIdNum;
        private String ipDatPath;
        private String hbaseZookeeperIp;
        private String hbaseTableName;


        @Override
        public Interceptor build() {
            return new FlumeDynamicApp(this.schemaHttpUrl,
                    this.appIdHttpUrl,
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
                this.appIdHttpUrl = context.getString("appIdHttpUrl", "");
                Preconditions.checkNotNull("".equals(appIdHttpUrl), "appIdHttpUrl must be set!!");
                logger.info("FlumeDynamicApp Read appIdHttpUrl from configuration : " + appIdHttpUrl);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("FlumeDynamicApp appIdHttpUrl invalid", e);
            } catch (Exception e) {
                logger.error("FlumeDynamicApp Get appIdHttpUrl is error : " + e);
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

