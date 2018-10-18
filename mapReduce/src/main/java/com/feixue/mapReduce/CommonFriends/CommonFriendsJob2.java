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
import java.util.Arrays;

/**
 * @Author ：feixue
 * @Data : 18:51 2018/10/18
 */
public class CommonFriendsJob2 {


    public static class CFMapper2 extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


//            A	I,K,C,B,G,F,H,O,D,

            String[] friendsAanUser = value.toString().split("\t");

            String friend = friendsAanUser[0];

            String[] users = friendsAanUser[1].split(",");

            // 将 user 进行排序，避免重复
            Arrays.sort(users); // 以用户-用户为 key，好友们做 value 传给 reduce

            for(int i=0; i<users.length-2; i++) {
                for(int j=i+1; j<users.length-1; j++) {
                    k.set(users[i] + "-" + users[j]);
                    v.set(friend);
                    context.write(k, v);
                }
            }


        }
    }

    public  static  class CFReduce2 extends Reducer<Text,Text,Text,Text> {


        Text v = new Text();

        @Override
        protected void reduce(Text user_user, Iterable<Text> iter, Context context) throws IOException, InterruptedException {

            // 传进来的数据 <用户 1-用户 2，好友们>
            // 新建 stringBuffer, 用于用户的共同好友们
            StringBuffer stringBuffer = new StringBuffer();
            // 遍历所有的好友，并将这些好友放在 stringBuffer 中，以" "分隔
            for (Text friend : iter) {
                stringBuffer.append(friend).append(" ");
            }
            // 以好友为 key,用户们为 value 传给下一个 mapper
            v.set(stringBuffer.toString());
            context.write(user_user,v);
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
        job.setMapperClass(CFMapper2.class);
        job.setReducerClass(CFReduce2.class);

        // 设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);


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
