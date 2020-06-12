package com.zdjizhi.flume.interceptor.common;

import java.util.Properties;


/**
 * @author Administrator
 */

public final class OnOffConfigurations {

    private static Properties propService = new Properties();


    public static String getStringProperty(Integer type, String key) {
        if (type == 0) {
            return propService.getProperty(key);
        } else {
            return null;
        }

    }

    public static Integer getIntProperty(Integer type, String key) {
        if (type == 0) {
            return Integer.parseInt(propService.getProperty(key));
        } else {
            return null;
        }
    }

    public static Long getLongProperty(Integer type, String key) {
        if (type == 0) {
            return Long.parseLong(propService.getProperty(key));
        } else {
            return null;
        }
    }

    public static Boolean getBooleanProperty(Integer type, String key) {
        if (type == 0) {
            return "true".equals(propService.getProperty(key).toLowerCase().trim());
        } else {
            return null;
        }
    }

    static {
        try {
            propService.load(OnOffConfigurations.class.getClassLoader().getResourceAsStream("service_flow_config.properties"));
        } catch (Exception e) {
            propService = null;
        }
    }
}
