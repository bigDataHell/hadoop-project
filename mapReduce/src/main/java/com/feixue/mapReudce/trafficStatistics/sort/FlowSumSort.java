package com.feixue.mapReudce.trafficStatistics.sort;

import java.io.IOException;


import com.feixue.mapReudce.trafficStatistics.FlowBean;
import com.feixue.mapReudce.trafficStatistics.partitioner.ProvincePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *  分区并且每个分区进行排序
 *
 */
public class FlowSumSort {
    
    public static class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
        
        Text v = new Text();
        FlowBean k = new FlowBean();
        
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            
                String line = value.toString();
                
                String[] fields = line.split("\t");
                
                String phoneNum = fields[0];
                
                long upFlow = Long.parseLong(fields[1]);
                
                long downFlow = Long.parseLong(fields[2]);
                
                k.set(upFlow, downFlow);
                v.set(phoneNum);
                
                context.write(k, v);
                
        }
        
    }
    
    
    public static class FlowSumSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
        
        @Override
        protected void reduce(FlowBean key, Iterable<Text> valus,Context context)
                throws IOException, InterruptedException {

            // 迭代器就一个数据
            context.write(valus.iterator().next(), key);
        }
        
    }
    
    
    public static void main(String[] args) throws Exception{
        

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //指定我这个 job 所在的 jar包位置
        job.setJarByClass(FlowSumSort.class);
        
        //指定我们使用的Mapper是那个类  reducer是哪个类
        job.setMapperClass(FlowSumSortMapper.class);
        job.setReducerClass(FlowSumSortReducer.class);
        
        // 设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        
        // 设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(6);

        // 设置分区的组件
        job.setPartitionerClass(ProvincePartitioner.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\flowsum\\output"));
        // 指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job, new Path("D:\\flowsum\\outputsortASC"));
        
        // 向 yarn 集群提交这个 job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
        
    }

}
