package com.feixue.sparkproject.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.feixue.sparkproject.dao.ITaskDAO;
import com.feixue.sparkproject.domain.Task;
import com.feixue.sparkproject.impl.DAOFactory;


/**
 * fastjson测试类
 * @Author ：feixue
 * @Data : 22:41 2018/10/11
 */
public class FastjsonTest {

    /**
     * 用一个task_param字段，来存储不同类型的任务的参数的方式。task_param字段中，
     * 实际上会存储一个任务所有的字段，使用JSON的格式封装所有任务参数，并存储在
     * task_param字段中。就实现了非常灵活的方式。
     如何来操作JSON格式的数据？
     比如说，要获取JSON中某个字段的值。我们这里使用的是阿里的fastjson工具包。
     使用这个工具包，可以方便的将字符串类型的JSON数据，转换为一个JSONObject对象，
     然后通过其中的getX()方法，获取指定的字段的值。
     * @param args
     */

    public static void main(String[] args) {

        String json = "[{'学生':'张三','班级':'一班','年级':'大一','科目':'高数','成绩':90},"
            + "{'学生':'李四','班级':'二班','年级':'大一','科目':'高数','成绩':80}]";

        String ss = "{\"studentName\":\"lily\",\"studentAge\":12}";
        JSONObject jsonObject1 = JSONObject.parseObject(ss);

        //JSONArray jsonArray = JSONArray.parseArray(json);
        //JSONObject jsonObject = jsonArray.getJSONObject(1);
       // System.out.println(jsonObject.getString("年级"));



    }
}
