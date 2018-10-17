package com.feixue.sparkproject.dao;

import com.feixue.sparkproject.domain.Task;

/**
 * 任务管理DAO接口
 * @Author ：feixue
 * @Data : 22:13 2018/10/11
 */
public interface ITaskDAO {

    /**
     * 根据主键查询业务
     */
    Task findById(Long taskid);
}
