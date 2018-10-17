package com.feixue.sparkproject.test;

import com.feixue.sparkproject.conf.ConfigurationManager;



import java.sql.*;


/**
 * JDBC原理介绍及增删改查示范
 *
 * @Author ：feixue
 * @Data : 15:00 2018/10/11
 *
 *
 * 步骤：
 *  1 加载数据库驱动类 Class.forName()
 *  2 获取数据库连接 DriverManager.getConnection(url,username.password)
 *  3 创建SQL语句执行句柄 Connection.createStatement()
 *  4 执行sql语句 Statement.executeUpdate(sql)
 *  5 释放数据库连接资源：finally,Connection.close()
 */
public class JdbcCRUD {

    //引用JDBC相关的所有接口或者是抽象类的时候，必须是引用java.sql包下的
    //java.sql包下的，才代表了java提供的JDBC接口，只是一套规范
    private static Connection conn = null;
    //定义SQL语句执行句柄:Statement对象
    //Statement对象其实就是底层基于Connection数据库连接
    private static Statement stmt = null;

    private static ResultSet rs = null;

    /**
     * 初始化配置
     */
    static {
        try {

            //第一步，加载数据库驱动
            //使用Class.forName()方式加载数据库的驱动类
            //Class.forName()是Java提供的一种基于反射的方式，直接根据类的全限定名（包+类）
            //从类所在的磁盘文件（.class文件）中加载类对应的内容，并创建对应的class对象

            Class.forName(ConfigurationManager.getProperty("jdbc.driver"));

            //获取数据库的连接
            //使用DriverManager.getConnection()方法获取针对数据库的连接
            //需要给方法传入三个参数，包括url、user、password
            //其中url就是有特定格式的数据库连接串，包括“主协议：子协议“//主机名：端口号//数据库”
            conn = DriverManager.getConnection(ConfigurationManager.getProperty("jdbc.url"),
                ConfigurationManager.getProperty("jdbc.user"),
                ConfigurationManager.getProperty("jdbc.password"));

            stmt = conn.createStatement();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws SQLException {

        query();
        //insert();
        //update();
        //delete();
       // preparedStatement();
    }

    /**
     * 测试查询数据
     */

    private static void query() throws SQLException {

        // 编写sql语句
        String sql = " select * from exam";
        // 执行查询语句
        rs = stmt.executeQuery(sql);
        // 遍历结果集
        while (rs.next()) {
            System.out.println(rs.getInt("id"));
            System.out.println(rs.getString("name"));
            System.out.println(rs.getDouble("english"));
            System.out.println(rs.getDouble("chinese"));
            System.out.println(rs.getDouble("math"));

        }

        // 释放数据库连接
        ReleaseDatabaseConnection();
    }

    /**
     * 测试插入数据
     */
    private static  void insert() throws SQLException {
        // sql语句
        String sql = " insert into exam(name,english,chinese,math) values ('xiaobai',44,55,66)";
        int line = stmt.executeUpdate(sql);
        System.out.println("影响了:"+line+"行数据");
        ReleaseDatabaseConnection();
    }

    /**
     * 测试更新数据
     */
    private  static void update() throws SQLException {
        // sql语句
        String sql = "update exam set math = 1000 where id = 4";
        int line = stmt.executeUpdate(sql);
        System.out.println("影响了:"+line+"行数据");
        ReleaseDatabaseConnection();
    }
    /**
     * 测试删除数据
     */
    private static void delete() throws SQLException {
        // sql语句
        String sql = "delete from exam where id = 6";
        int line = stmt.executeUpdate(sql);
        System.out.println("影响了:"+line+"行数据");
        ReleaseDatabaseConnection();
    }

    /**
     * 使用Statement时，必须在SQL语句中，实际地区嵌入值，容易发生SQL注入
     * 而且性能低下
     *
     * 使用PreparedStatement，就可以解决上述的两个问题
     * 1.SQL注入，使用PreparedStatement时，是可以在SQL语句中，对值所在的位置使用？这种占位符，
     * 实际的值是放在数组中的参数，PreparedStatement会对数值做特殊处理，往往处理后会使恶意注入的SQL代码失效。
     * 2.提升性能，使用P热怕热的Statement后，结构类似的SQL语句会变成一样的，因为值的地方会变成？，
     * 一条SQL语句，在MySQL中只会编译一次，后面的SQL语句过来，就直接拿编译后的执行计划加上不同的参数直接执行，
     * 可以大大提升性能
     */
    private  static  void preparedStatement() throws SQLException {

        //第一个，SQL语句中，值所在的地方，都用问号代表
        String sql ="insert into exam(name,english,chinese,math) values (?,?,?,?)";

        PreparedStatement pstmt = conn.prepareStatement(sql);

        //第二个，必须调用PreparedStatement的setX（）系列方法，对指定的占位符赋值
        pstmt.setString(1, "李四");
        pstmt.setInt(2, 26);
        pstmt.setInt(3,27);
        pstmt.setInt(4,28);


        //第三个，执行SQL语句时，直接使用executeUpdate（）即可
        int line = pstmt.executeUpdate();

        System.out.println("影响了:"+line+"行数据");

        if(pstmt != null){
            pstmt.close();
        }
        if(conn != null ){
            conn.close();
        }



    }

    /**
     * 释放数据库连接
     */
    private  static void  ReleaseDatabaseConnection() throws SQLException {
        if(stmt != null){
            stmt.close();
        }
        if(conn != null){
            conn.close();
        }
        System.out.println("数据库连接释放完成");
    }
}
