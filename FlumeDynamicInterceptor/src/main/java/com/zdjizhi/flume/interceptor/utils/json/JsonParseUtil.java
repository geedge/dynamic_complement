package com.zdjizhi.flume.interceptor.utils.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.flume.interceptor.utils.http.HttpClientUtil;
import net.sf.cglib.beans.BeanGenerator;
import net.sf.cglib.beans.BeanMap;

import java.util.*;

/**
 * 使用fastjson解析json的工具类
 */
public class JsonParseUtil {

    /**
     * 模式匹配，给定一个类型字符串返回一个类类型
     *
     * @param type
     * @return
     */

    public static Class getClassName(String type) {
        Class clazz;

        switch (type) {
            case "int":
                clazz = Integer.class;
                break;
            case "String":
                clazz = String.class;
                break;
            case "long":
                clazz = long.class;
                break;
            case "Integer":
                clazz = Integer.class;
                break;
            case "double":
                clazz = double.class;
                break;
            case "float":
                clazz = float.class;
                break;
            case "char":
                clazz = char.class;
                break;
            case "byte":
                clazz = byte.class;
                break;
            case "boolean":
                clazz = boolean.class;
                break;
            case "short":
                clazz = short.class;
                break;
            default:
                clazz = String.class;
        }
        return clazz;
    }

    /**
     * 根据反射生成对象的方法
     *
     * @param properties
     * @return 生成的Object类型的对象
     */
    public static Object generateObject(Map properties) {
        BeanGenerator generator = new BeanGenerator();
        Set keySet = properties.keySet();
        for (Iterator i = keySet.iterator(); i.hasNext(); ) {
            String key = (String) i.next();
            generator.addProperty(key, (Class) properties.get(key));
        }
        return generator.create();
    }

    /**
     * 获取属性值的方法
     *
     * @param obj
     * @param property
     * @return 属性的值
     */
    public static Object getValue(Object obj, String property) {
        BeanMap beanMap = BeanMap.create(obj);
        return beanMap.get(property);
    }

    /**
     * 更新属性值的方法
     *
     * @param obj
     * @param property
     * @param value
     */
    public static void setValue(Object obj, String property, Object value) {
        BeanMap beanMap = BeanMap.create(obj);
        beanMap.put(property, value);
    }

    /**
     * 通过获取String类型的网关schema链接来获取map，用于生成一个Object类型的对象
     *
     * @param http
     * @return 用于反射生成schema类型的对象的一个map集合
     */
    public static HashMap<String, Class> getMapFromhttp(String http) {
        HashMap<String, Class> map = new HashMap<>();

        String schema = HttpClientUtil.requestByGetMethod(http);
        Object data = JSON.parseObject(schema).get("data");

        //获取fields，并转化为数组，数组的每个元素都是一个name doc type
        JSONObject schemaJson = JSON.parseObject(data.toString());
        JSONArray fields = (JSONArray) schemaJson.get("fields");

        for (Object field : fields) {
            String name = JSON.parseObject(field.toString()).get("name").toString();
            String type = JSON.parseObject(field.toString()).get("type").toString();
            map.put(name, getClassName(type));

        }


        return map;
    }


    /**
     * 根据http链接获取schema，解析之后返回一个任务列表 (useList toList funcList)
     *
     * @param http
     * @return
     */
    public static ArrayList<String[]> getJobListFromHttp(String http) {
        ArrayList<String[]> list = new ArrayList<>();

        String schema = HttpClientUtil.requestByGetMethod(http);
        //解析data
        Object data = JSON.parseObject(schema).get("data");

        //获取fields，并转化为数组，数组的每个元素都是一个name doc type
        JSONObject schemaJson = JSON.parseObject(data.toString());
        JSONArray fields = (JSONArray) schemaJson.get("fields");

        for (Object field : fields) {
            Object doc = JSON.parseObject(field.toString()).get("doc");
            String name = JSON.parseObject(field.toString()).get("name").toString();

            if (doc != null) {
                Object format = JSON.parseObject(doc.toString()).get("format");

                if (format != null) {

                    String functions = null;
                    String appendTo = null;
                    String params = null;
                    Object functionsObj = JSON.parseObject(format.toString()).get("functions");
                    Object appendToObj = JSON.parseObject(format.toString()).get("appendTo");
                    Object paramObj = JSON.parseObject(format.toString()).get("param");

                    if (functionsObj != null) {
                        functions = functionsObj.toString();
                    }

                    if (appendToObj != null) {
                        appendTo = appendToObj.toString();
                    }
                    if (paramObj != null) {
                        params = paramObj.toString();
                    }


                    if (appendTo != null && params == null) {
                        String[] functionArray = functions.split(",");
                        String[] appendToArray = appendTo.split(",");

                        for (int i = 0; i < functionArray.length; i++) {
                            list.add(new String[]{name, appendToArray[i], functionArray[i], null});

                        }
                    } else if (appendTo != null && params != null) {
                        String[] functionArray = functions.split(",");
                        String[] appendToArray = appendTo.split(",");
                        String[] paramArray = params.split(",");

                        for (int i = 0; i < functionArray.length; i++) {
                            list.add(new String[]{name, appendToArray[i], functionArray[i], paramArray[i]});

                        }
                    } else {
                        list.add(new String[]{name, name, functions, params});
                    }

                }
            }

        }
        return list;
    }


}