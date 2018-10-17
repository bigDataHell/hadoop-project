package com.feixue.sparkproject.test;

import com.feixue.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC辅助组件测试类
 *
 * @Author ：feixue
 * @Data : 20:06 2018/10/11
 */
public class JDBCHelperTest {

    public static void main(String[] args) {
        // 获取JDBCHelper单例对象
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        //测试普通的增删改语句
//        jdbcHelper.executeUpdate("insert into exam(name,english,chinese,math) values(?,?,?,?)",
//                                    new Object[]{"xiaohua",1,22,33});

        //测试查询语句
        final Map<String, Object> testUser = new HashMap<String, Object>();
/*
//      //设计一个内部接口QueryCallback
//      //那么在执行查询语句的时候，我们就可以封装和指定自己的查询结果的处理逻辑
//      //封装在一个内部接口的匿名内部类对象中，传入JDBCHelper的方法
//      //在方法内部，可以回调我们定义的逻辑，处理查询结果
//      //并将查询结果放入外部的变量中

        jdbcHelper.executeQuery("select * from exam where id = ?", new Object[]{5},
            new JDBCHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if(rs.next()){
                        String name = rs.getString("name");
                        int english = rs.getInt("english");
                        int chinese = rs.getInt("chinese");
                        int math = rs.getInt("math");

                        //匿名内部类的使用，有一个很重要的知识点
                        //如果要访问外部类中的一些成员，比如方法内的局部变量
                        //那么，必须将局部变量声明为final类型才能访问，
                        //否则是访问不了的

                        testUser.put("name",name);
                        testUser.put("english",english);
                        testUser.put("chinese",chinese);
                        testUser.put("math",math);
                    }
                }
            });
        System.out.println("name : "+testUser.get("name") +"\n" +
                        "英语 ："+testUser.get("english")+
                        "语文 ："+testUser.get("chinese")+
                        "数学 ："+testUser.get("math"));*/

        //测试批量执行SQL语句

        String sql = "insert into exam(name,english,chinese,math) values (?,?,?,?)";
        List<Object[]> paramsList = new ArrayList<Object[]>();
        paramsList.add(new Object[]{"王老吉",34,23,56});
        paramsList.add(new Object[]{"王老二",334,323,356});
        jdbcHelper.executeBatch(sql,paramsList);

    }
}
