package com.zdjizhi.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.zdjizhi.flume.interceptor.bean.Knowledge;
import com.zdjizhi.flume.interceptor.common.OnOffConfig;
import com.zdjizhi.utils.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;


/**
 * @author qidaijie
 */
public class FlumeOnOffApp implements Interceptor {
    private static Logger logger = Logger.getLogger(FlumeOnOffApp.class);

    @Override
    public void initialize() {

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
            message = parsingMessage(message);
            if (StringUtils.isNotBlank(message)) {
                event.setBody(message.getBytes());
                return event;
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.error("FlumeOnOffApp intercept(Event event) method is error===>{" + e + "}<===");
            e.printStackTrace();
        }
        return null;
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
        logger.warn("FlumeOnOffApp is closed.");
    }

    /**
     * 解析日志，并补全
     * 补domain,补subscriber_id
     *
     * @param message Security原始日志
     * @return 补全后的日志
     * <p>
     */
    private String parsingMessage(String message) {
        if (StringUtil.isNotBlank(message)) {
            JSONObject jsonObject = JSONObject.parseObject(message);
            //数据需包含 radius_packet_type  and radius_acct_status_type 字段
            if (jsonObject.containsKey(OnOffConfig.RADIUS_PACKET_TYPE) && jsonObject.containsKey(OnOffConfig.RADIUS_ACCT_STATUS_TYPE)) {
                int packetType = jsonObject.getInteger(OnOffConfig.RADIUS_PACKET_TYPE);
                int statusType = jsonObject.getInteger(OnOffConfig.RADIUS_ACCT_STATUS_TYPE);
                //条件radius_packet_type = 4  and radius_acct_status_type = 1 or 2
//                    boolean existed = OnOffConfig.ACCOUNTING_REQUEST == packetType && (OnOffConfig.START_BILLING == statusType || OnOffConfig.STOP_BILLING == statusType);
                if (OnOffConfig.ACCOUNTING_REQUEST == packetType && (OnOffConfig.START_BILLING == statusType || OnOffConfig.STOP_BILLING == statusType)) {

                    Knowledge knowledge = new Knowledge();
                    knowledge.setFramed_ip(jsonObject.getString("radius_framed_ip"));
                    knowledge.setAccount(jsonObject.getString("radius_account"));
                    knowledge.setAcct_status_type(statusType);

                    /*
                     *如果存在时间戳则选择此时间戳没有获取当前时间
                     */
                    if (jsonObject.containsKey(OnOffConfig.RADIUS_EVENT_TIMESTAMP)) {
                        knowledge.setEvent_timestamp(jsonObject.getInteger("radius_event_timestamp"));
                    } else {
                        knowledge.setEvent_timestamp((System.currentTimeMillis() / 1000));
                    }

                    /*
                     * 标识同一个连接：
                     * 1.数据若存在acct_multi_session_id属性，取该属性
                     * 2. 不存在取 acct_session_id
                     */
                    if (jsonObject.containsKey(OnOffConfig.RADIUS_MULTI_SESSION_ID)) {
                        knowledge.setAcct_session_id(jsonObject.getString("radius_acct_multi_session_id"));
                    } else {
                        knowledge.setAcct_session_id(jsonObject.getString("radius_acct_session_id"));
                    }

                    /*
                     *用户的在线时长，以秒为单位,下线用户无此属性，默认为0
                     */
                    if (jsonObject.containsKey(OnOffConfig.RADIUS_SESSION_TIME)) {
                        knowledge.setAcct_session_time(jsonObject.getInteger("radius_acct_session_time"));
                    } else {
                        knowledge.setAcct_session_time(0);
                    }

                    return JSONObject.toJSONString(knowledge);
                }
            }
        }
        return null;
    }

    public static class FlumeDynamicAppBuilder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new FlumeOnOffApp();
        }

        @Override
        public void configure(Context context) {

        }

    }

}

