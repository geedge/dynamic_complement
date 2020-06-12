package com.zdjizhi.flume.interceptor.utils.http;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 获取网关schema工具类
 */
public class HttpClientUtil {
    public static String requestByGetMethod(String s) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        StringBuilder entityStringBuilder = null;
        try {
            HttpGet get = new HttpGet(s);
            CloseableHttpResponse httpResponse = null;
            httpResponse = httpClient.execute(get);
            try {
                HttpEntity entity = httpResponse.getEntity();
                entityStringBuilder = new StringBuilder();
                if (null != entity) {
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent(), "UTF-8"), 8 * 1024);
                    String line = null;
                    while ((line = bufferedReader.readLine()) != null) {
                        entityStringBuilder.append(line);
                    }
                }
            } finally {
                httpResponse.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (httpClient != null) {
                    httpClient.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return entityStringBuilder.toString();
    }


//    public static void main(String[] args) {
////        String s = HttpClientUtil.requestByGetMethod("http://192.168.40.224:9999/metadata/schema/v1/fields/security_event_log");
////        System.out.println(s);
////        String schemaHttpRes = HttpClientUtil.requestByGetMethod("http://192.168.40.224:9999/metadata/schema/v1/fields/security_event_log");
////        String schemaHttpRes = HttpClientUtil.requestByGetMethod("http://192.168.40.151:9999/metadata/schema/v1/fields/security_event_log");
//        String schemaHttpRes = HttpClientUtil.requestByGetMethod("http://192.168.40.224:9999/metadata/schema/v1/fields/connection_record_log");
//        String data = JSON.parseObject(schemaHttpRes).get("data").toString();
//        System.out.println(data);
//    }
}
