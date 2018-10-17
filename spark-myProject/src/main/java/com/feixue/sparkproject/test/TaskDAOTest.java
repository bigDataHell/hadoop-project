package com.feixue.sparkproject.test;

import com.feixue.sparkproject.dao.ITaskDAO;
import com.feixue.sparkproject.domain.Task;
import com.feixue.sparkproject.impl.DAOFactory;


/**
 * @Author ：feixue
 * @Data : 22:25 2018/10/11
 */
public class TaskDAOTest {
    public static void main(String[] args) {

        ITaskDAO taskDao = DAOFactory.getTaskDao();
        long id = 1;
        Task task = taskDao.findById(id);
        System.out.println(task.toString());
    }
}
