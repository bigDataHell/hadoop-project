package com.feixue.mapReduce.CommonFriends;

import com.feixue.mapReduce.trafficStatistics.sort.FlowSumSort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @Author ：feixue
 * @Data : 16:20 2018/10/18
 */
public class CommonFriendsJob {


    public static class CFMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 传进来的 value: A:B,C,D,F,E,O
            // 将 value 转换成字符串,line 表示行数据，为"A:B,C,D,F,E,O"

            String line = value.toString();
            String[] fields = line.split(":");

            // 用户
            String user = fields[0];
            // 该用户的所有好友
            String[] friends = fields[1].split(",");

            for (String friend : friends) {
                k.set(friend);
                v.set(user);
                context.write(k,v);
            }

        }
    }

    public  static  class CFReduce extends Reducer<Text,Text,Text,Text> {

        Text k = new Text();
        Text v = new Text();

        @Override
        protected void reduce(Text friend, Iterable<Text> iter, Context context) throws IOException, InterruptedException {

            StringBuffer buffer = new StringBuffer();

            for (Text value : iter) {
                buffer.append(value).append(",");
            }

            k.set(friend);
            v.set(buffer.toString());
            context.write(k,v);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args == null || args.length < 2) {
            System.err.println("Parameter Errors! Usages:<inputpath> <outputpath> ");
            System.exit(-1);
        }

        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);

        //指定我这个 job 所在的 jar包位置
        job.setJarByClass(FlowSumSort.class);

        //指定我们使用的Mapper是那个类  reducer是哪个类
        job.setMapperClass(CFMapper.class);
        job.setReducerClass(CFReduce.class);

        // 设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        //D:\DataSource\friends  D:\\flowsum\\outputsortFriend
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // ReduceTask必须设置为1
        job.setNumReduceTasks(1);

        // 向 yarn 集群提交这个 job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
