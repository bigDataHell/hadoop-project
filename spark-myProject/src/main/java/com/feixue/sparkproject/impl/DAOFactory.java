package com.feixue.sparkproject.impl;

import com.feixue.sparkproject.dao.ITaskDAO;

/**
 * Dao工厂类
 * @Author ：feixue
 * @Data : 22:35 2018/10/11
 */
public class DAOFactory {
    /**
     * 获取任务管理的dao
     */
    public static ITaskDAO getTaskDao(){
        return new TaskDAOImpl();
    }
}
