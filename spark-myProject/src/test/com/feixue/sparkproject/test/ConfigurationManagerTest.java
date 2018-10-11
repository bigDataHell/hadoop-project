package com.feixue.sparkproject.test;

import conf.ConfigurationManager;

/**
 *
 * 配置管理组件测试类
 * @Author ：feixue
 * @Data : 14:50 2018/10/11
 */
public class ConfigurationManagerTest {

    public static void main(String[] args) {

        String value1 = ConfigurationManager.getProperties("baimao");
        String value2 = ConfigurationManager.getProperties("xiaobai");
        System.out.println(value1);
        System.out.println(value2);


    }
}
