package com.feixue.sparkproject.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 *
 * @author feixue
 */
public class ConfigurationManager {

    private static Properties properties = new Properties();

    /**
     * 静态代码块
     */
    static {

        InputStream resourceAsStream = ConfigurationManager
            .class.getClassLoader()
            .getResourceAsStream("my.properties");
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {

        }
    }

    /**
     * 获取指定key对应的value
     */
    public  static String getProperty(String key){

        return properties.getProperty(key);

    }

    /**
     * 返回正整数
     * @param key
     * @return
     */
    public static Integer getInteger(String key){

        try {
            String value = getProperty(key);
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;

    }


    /**
     * 获取布尔类型的配置项
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
