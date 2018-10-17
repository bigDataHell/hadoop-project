package com.feixue.sparkproject.spark.java;

import com.alibaba.fastjson.JSONObject;
import com.feixue.sparkproject.conf.ConfigurationManager;
import com.feixue.sparkproject.constant.Constants;
import com.feixue.sparkproject.dao.ITaskDAO;
import com.feixue.sparkproject.domain.Task;
import com.feixue.sparkproject.impl.DAOFactory;
import com.feixue.sparkproject.test.MockData;
import com.feixue.sparkproject.utils.DateUtils;
import com.feixue.sparkproject.utils.ParamUtils;

import com.feixue.sparkproject.utils.StringUtils;
import groovy.lang.Tuple;
import org.apache.ivy.core.search.SearchEngine;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;
import scala.annotation.meta.param;

import javax.swing.*;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * 用户访问session分析spark作业
     *
     * 接收用户创建的分析任务，用户可能指定的条件如下：
     * 1.时间范围：起始日期-结束日期
     * 2.性别：男或女
     * 3.年龄范围
     * 4.职业：多选
     * 5.城市：多选
     * 6.搜索词：多个搜索词，只要某个session中的任何一个
     *   action搜索过指定的关键词，那么session就符合条件
     * 7.点击品类：多个品类，只要某个session中的任何一个
     *   action点击过某个品类，那么session就符合条件
     *
     * 我们的Spark作业如何接受用户创建的任务呢？
     * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，
     * 任务参数以JSON格式封装在task_param字段中
     * 接着J2EE平台执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
     * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数传递给spark作业的main函数
     * 参数就封装在main函数得到args数组中
     *
     * 这是spark本事提供的特性
 *
 * [0,user0,name0,8,professional51,city40,female]
 *
 * [2018-10-12,28,7393925d1a1e4db98923a93b0ba8de27,7,2018-10-12 11:16:05,null,null,null,null,null,9,49]
 *
 *
 *
     *
     * @Author ：feixue
 * @Data : 23:05 2018/10/11
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        //构建spark上下文
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME)
            .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟测试数据
        mockData(sc, sqlContext);

        //创建需要的DAO组件
        ITaskDAO taskDao = DAOFactory.getTaskDao();

        //首先查询出来指定的任务,并获取任务的查询参数,
        Long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDao.findById(taskId);
        // {"startDate":["2018-10-15"],"endDate":["2018-10-16"]}
        System.out.println(task.getTaskParam());
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //如果要进行session粒度的数据聚合，
        //首先要从user_visit_action表中，查询出来指定日期范围内的数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

//        1fa88790dec148329a922fa0c64e1ba2 : [2018-10-15,63,1fa88790dec148329a922fa0c64e1ba2,0,2018-10-15 14:16:02,新辣道鱼火锅,null,null,null,null,null,null]
//        1fa88790dec148329a922fa0c64e1ba2 : [2018-10-15,63,1fa88790dec148329a922fa0c64e1ba2,7,2018-10-15 14:56:05,null,29,27,null,null,null,null]
//        1fa88790dec148329a922fa0c64e1ba2 : [2018-10-15,63,1fa88790dec148329a922fa0c64e1ba2,5,2018-10-15 14:26:15,null,null,null,81,60,null,null]
//        1fa88790dec148329a922fa0c64e1ba2 : [2018-10-15,63,1fa88790dec148329a922fa0c64e1ba2,2,2018-10-15 14:10:36,重庆辣子鸡,null,null,null,null,null,null]
        JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);



        //聚合
        //首先，可以将行为数据按照session_id进行groupByKey分组
        //此时的数据粒度就是session粒度了，然后可以将session粒度的数据与用户信息数据进行join
        //然后就可以获取到session粒度的数据，同时数据里面还包含了session对应的user信息
        //到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,
        // clickCategoryIds,age,professional,city,sex)
        JavaPairRDD<String, String> sessionid2AggrInfoRDD =
            aggregateBySession(sqlContext, actionRDD);

//960b7478a0c24194a3aba291d9cff191
//sessionid=960b7478a0c24194a3aba291d9cff191|searchKeywords=温泉,国贸大厦,重庆辣子鸡,火锅|clickCategoryIds=6,47,0,89,11,27,81|visitLength=2894|stepLength=23|starttime=2018-10-15|age=19|professional=professional30|city=city38|sex=male

//        for (Tuple2<String, String> s :sessionid2AggrInfoRDD.take(10)) {
//            System.out.println("--------------");
//            System.out.println(s._1());
//            System.out.println(s._2());
//            System.out.println(s._1);
//        }


        //接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        //相当于我们自己编写的算子，是要访问外面的任务参数对象的
        //匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的

        //重构，同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
            "", new SesssionAggrStatAccumulator());


        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
            sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

        //关闭spark上下文
        sc.close();
    }

    /**
     * 获取SQLContext
     * 如果在本地测试环境的话，那么久生成SQLContext对象
     *如果在生产环境运行的话，那么就生成HiveContext对象
     * @param sc SparkContext
     * @return SQLContext
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if(local){
            return new SQLContext(sc);
        }else{
            System.out.println("+================================+++++++");
            return new HiveContext(sc);
        }

    }

    /**
     * 生成模拟数据
     * 只有是本地模式，才会生成模拟数据
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     * @param sqlContext SQLContext
     * @param taskParam 任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(
        SQLContext sqlContext, JSONObject taskParam) {

        //先在Constants.java中添加任务相关的常量
        //String PARAM_START_DATE = "startDate";
        //String PARAM_END_DATE = "endDate";
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        System.out.println(startDate);
        System.out.println(endDate);

        // todo
        String sql = "select * "
            + "from user_visit_action "
            + "where date>='" + startDate + "'"
            + "and date<='" + endDate + "'";

        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();
    }

    /**
     * 获取sessionid2到访问行为数据的映射的RDD
     * @param actionRDD
     * @return
     */
    public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>(){

            private static final long serialVersionUID = 1L;

            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }

        });

    }


    /**
     * 对行为数据按sesssion粒度进行聚合
     * @param actionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggregateBySession(
        SQLContext sqlContext, JavaRDD<Row> actionRDD) {
        //现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或者搜索
        //现在需要将这个Row映射成<sessionid,Row>的格式
        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(

            /**
             * PairFunction
             * 第一个参数，相当于是函数的输入
             * 第二个参数和第三个参数，相当于是函数的输出（Tuple），分别是Tuple第一个和第二个值
             */
            new PairFunction<Row, String, Row>() {

                private static final long serialVersionUID = 1L;

                public Tuple2<String, Row> call(Row row) throws Exception {

                    //按照MockData.java中字段顺序获取
                    //此时需要拿到session_id，序号是2
                    return new Tuple2<String, Row>(row.getString(2), row);
                }

            });

        //对行为数据按照session粒度进行分组

//        87645eb5f2f34a21be3e91b9542d856a :
//[2018-10-15,56,87645eb5f2f34a21be3e91b9542d856a,4,2018-10-15 14:42:48,null,null,null,81,84,null,null]
//[2018-10-15,56,87645eb5f2f34a21be3e91b9542d856a,5,2018-10-15 14:08:32,日本料理,null,null,null,null,null,null]
//[2018-10-15,56,87645eb5f2f34a21be3e91b9542d856a,8,2018-10-15 14:56:43,null,30,50,null,null,null,null]
//[2018-10-15,56,87645eb5f2f34a21be3e91b9542d856a,0,2018-10-15 14:16:19,温泉,null,null,null,null,null,null]
//[2018-10-15,56,87645eb5f2f34a21be3e91b9542d856a,1,2018-10-15 14:30:04,null,85,47,null,null,null,null]
//[2018-10-15,56,87645eb5f2f34a21be3e91b9542d856a,4,2018-10-15 14:28:00,null,null,null,null,null,30,18]
//[2018-10-15,56,87645eb5f2f34a21be3e91b9542d856a,1,2018-10-15 14:15:21,重庆辣子鸡,null,null,null,null,null,null]
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD =
            sessionid2ActionRDD.groupByKey();

//        List<Tuple2<String, Iterable<Row>>> take = sessionid2ActionsRDD.take(10);
//        for(Tuple2<String, Iterable<Row>> ss : take){
//            System.out.println(ss._1()+" : ");
//            for(Row sss : ss._2()){
//                System.out.println(sss.toString());
//            }
//            System.out.println("======================");
//        }

        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        //到此为止，获取的数据格式如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
            new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                private static final long serialVersionUID = 1L;

                public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
                    throws Exception {
                    String sessionid = tuple._1;
                    Iterator<Row> iterator = tuple._2.iterator();

                    StringBuffer searchKeywordsBuffer = new StringBuffer("");
                    StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                    Long userid = null;

//[2018-10-15,56,87645eb5f2f34a21be3e91b9542d856a,1,2018-10-15 14:15:21,重庆辣子鸡,null,null,null,null,null,null]
                    //session的起始和结束时间
                    Date startTime = null;
                    Date endTime = null;
                    //session的访问步长
                    int stepLength = 0;

                    //遍历session所有的访问行为
                    while(iterator.hasNext()) {
                        //提取每个 访问行为的搜索词字段和点击品类字段
                        Row row = iterator.next();
                        if(userid == null) {
                            userid = row.getLong(1);
                        }
                        String searchKeyword = row.getString(5);
                        Long clickCategoryId = row.getLong(6);

                        //实际上这里要对数据说明一下
                        //并不是每一行访问行为都有searchKeyword和clickCategoryId两个字段的
                        //其实，只有搜索行为是有searchKeyword字段的
                        //只有点击品类的行为是有clickCaregoryId字段的
                        //所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                        //所以是否将搜索词点击品类id拼接到字符串中去
                        //首先要满足不能是null值
                        //其次，之前的字符串中还没有搜索词或者点击品类id

                        if(StringUtils.isNotEmpty(searchKeyword)) {
                            if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                searchKeywordsBuffer.append(searchKeyword + ",");
                            }
                        }
                        if(clickCategoryId != null) {
                            if(!clickCategoryIdsBuffer.toString().contains(
                                String.valueOf(clickCategoryId))) {
                                clickCategoryIdsBuffer.append(clickCategoryId + ",");
                            }
                        }
//[2018-10-15,56,87645eb5f2f34a21be3e91b9542d856a,1,2018-10-15 14:15:21,重庆辣子鸡,null,null,null,null,null,null]
                        //计算session开始和结束时间
                        Date actionTime = DateUtils.parseTime(row.getString(4));
                        if(startTime == null) {
                            startTime = actionTime;
                        }
                        if(endTime == null) {
                            endTime = actionTime;
                        }
                        //测试此日期是否在指定日期之前。
                        if(actionTime.before(startTime)) {
                            startTime = actionTime;
                        }
                        if(actionTime.after(endTime)) {
                            endTime = actionTime;
                        }

                        //计算session访问步长
                        // 用户点击或者搜素的总次数
                        stepLength ++;
                    }

                    //计算session开始和结束时间
                    //现在DateUtils.java中添加方法
                    //public static Date parseTime(String time) {
                    //	try {
                    //		return TIME_FORMAT.parse(time);
                    //	} catch (ParseException e) {
                    //		e.printStackTrace();
                    //	}
                    //	return null;
                    //}



                    //StringUtils引入的包是import com.erik.sparkproject.util.trimComma;
                    String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                    String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());


                    //计算session访问时长（秒）
                    long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                    //返回的数据即是<sessionid, partAggrInfo>
                    //但是，这一步聚合后，其实还需要将每一行数据，根对应的用户信息进行聚合
                    //问题来了，如果是跟用户信息进行聚合的话，那么key就不应该是sessionid，而应该是userid
                    //才能够跟<userid, Row>格式的用户信息进行聚合
                    //如果我们这里直接返回<sessionid, partAggrInfo>,还得再做一次mapToPair算子
                    //将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

                    //所以，我们这里其实可以直接返回数据格式就是<userid,partAggrInfo>
                    //然后在直接将返回的Tuple的key设置成sessionid
                    //最后的数据格式，还是<sessionid,fullAggrInfo>

                    //聚合数据，用什么样的格式进行拼接？
                    //我们这里统一定义，使用key=value|key=vale


                    //结束时间不用写，因为开始时间+session时长就等于结束时间。


                    //在Constants.java中定义spark作业相关的常量
                    //String FIELD_SESSION_ID = "sessionid";
                    //String FIELD_SEARCH_KEYWORDS = "searchKeywords";
                    //String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
                    //String FIELD_VISIT_LENGTH = "visitLength";
                    //String FIELD_STEP_LENGTH = "stepLength";
                    //String FIELD_START_TIME = "starttime";
                    String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                        + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                        + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                        + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                        + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                        + Constants.FIELD_START_TIME + "=" + DateUtils.formatDate(startTime);



                    return new Tuple2<Long, String>(userid, partAggrInfo);
                }

            });

//        28
//        sessionid=64b8b3b6c1bb48529c8f0419803bae18|searchKeywords=国贸大厦,蛋糕|clickCategoryIds=0,8,18|visitLength=2162|stepLength=6|starttime=2018-10-15

//        List<Tuple2<Long, String>> take = userid2PartAggrInfoRDD.take(10);
//        for (Tuple2<Long, String> longStringTuple2 : take) {
//            System.out.println("------------");
//            System.out.println(longStringTuple2._1());
//            System.out.println(longStringTuple2._2());
//
//        }


        // todo 用户数据从哪里来
        //查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();


        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
            new PairFunction<Row, Long, Row>(){

                private static final long serialVersionUID = 1L;

                public Tuple2<Long, Row> call(Row row) throws Exception {
                    return new Tuple2<Long, Row>(row.getLong(0), row);
                }

            });

        //将session粒度聚合数据，与用户信息进行join
        // <userId,Row> <userId,String>
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
            userid2PartAggrInfoRDD.join(userid2InfoRDD);



        //对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(

            new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

                private static final long serialVersionUID = 1L;

                public Tuple2<String, String> call(
                    Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                    String partAggrInfo = tuple._2._1;
                    Row userInfoRow = tuple._2._2;

                    String sessionid = StringUtils.getFieldFromConcatString(
                        partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                    int age = userInfoRow.getInt(3);
                    String professional = userInfoRow.getString(4);
                    String city = userInfoRow.getString(5);
                    String sex = userInfoRow.getString(6);

                    //在Constants.java中添加以下常量
                    //String FIELD_AGE = "age";
                    //String FIELD_PROFESSIONAL = "professional";
                    //String FIELD_CITY = "city";
                    //String FIELD_SEX = "sex";
                    String fullAggrInfo = partAggrInfo + "|"
                        + Constants.FIELD_AGE + "=" + age + "|"
                        + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                        + Constants.FIELD_CITY + "=" + city + "|"
                        + Constants.FIELD_SEX + "=" + sex ;
                    return new Tuple2<String, String>(sessionid, fullAggrInfo);
                }


            });
        return sessionid2FullAggrInfoRDD;
    }


}
