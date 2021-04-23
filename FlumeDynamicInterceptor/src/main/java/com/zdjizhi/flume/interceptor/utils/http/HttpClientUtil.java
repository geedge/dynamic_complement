package com.zdjizhi.flume.interceptor.utils.http;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
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
 * @author qidaijie
 */
public class HttpClientUtil {
    private static final Log logger = LogFactory.get();

    /**
     * 请求网关获取schema
     *
     * @param http 网关url
     * @return schema
     */
    public static String requestByGetMethod(String http) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        StringBuilder entityStringBuilder;

        HttpGet get = new HttpGet(http);
        BufferedReader bufferedReader = null;
        CloseableHttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.execute(get);
            HttpEntity entity = httpResponse.getEntity();
            entityStringBuilder = new StringBuilder();
            if (null != entity) {
                bufferedReader = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent(), "UTF-8"), 8 * 1024);
                int intC;
                while ((intC = bufferedReader.read()) != -1) {
                    char c = (char) intC;
                    if (c == '\n') {
                        break;
                    }
                    entityStringBuilder.append(c);
                }

                return entityStringBuilder.toString();
            }
        } catch (IOException e) {
            logger.error("Get Schema from Query engine ERROR! Exception message is:" + e);
        } finally {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    logger.error("Close HTTP Client ERROR! Exception messgae is:" + e);
                }
            }
            if (httpResponse != null) {
                try {
                    httpResponse.close();
                } catch (IOException e) {
                    logger.error("Close httpResponse ERROR! Exception messgae is:" + e);
                }
            }
            if (bufferedReader != null) {
                org.apache.commons.io.IOUtils.closeQuietly(bufferedReader);
            }
        }
        return "";
    }
}
