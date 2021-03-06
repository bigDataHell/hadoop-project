package com.feixue.sparkproject.impl;

import com.feixue.sparkproject.dao.ITaskDAO;
import com.feixue.sparkproject.domain.Task;
import com.feixue.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;

/**
 * 任务管理DAO实现类
 *
 * @Author ：feixue
 * @Data : 22:15 2018/10/11
 */
public class TaskDAOImpl implements ITaskDAO {

    /**
     * 根据主键查询任务
     */
    @Override
    public Task findById(Long taskid) {

        final  Task task = new Task();

        String sql = "select * from task where task_id = ?";

        Object[] params = new Object[]{taskid};

        JDBCHelper.getInstance().executeQuery(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next()){

                    long taskid = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskid(taskid);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });

        return task;
    }



}
