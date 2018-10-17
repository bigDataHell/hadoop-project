package com.feixue.mapReudce.trafficStatistics;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 *  提取字段 ：
 *
 *   手机号， 上行流量， 下行流量
 *
 * @Author ：feixue
 * @Data : 19:02 2018/10/17
 */
public class FlowSumMapper extends Mapper<LongWritable,Text,FlowBean,Text> {

    private FlowBean v = new FlowBean();

    private Text k = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line  = value.toString();
        // 制表符
        String[] fields = line.split("\t");
        // 手机号
        String phoneNum = fields[1];

        //观察数据，发现一些数据缺失域名字段，但是数据切分之后获得的数据长度不变
        // 那么可以从后面往前获取数据

        Long upFlow = Long.parseLong(fields[fields.length-3]);

        Long downFlow = Long.parseLong(fields[fields.length-2]);

        k.set(phoneNum);
        // 再FlowBean中添加一set方法
        v.set(upFlow,downFlow);

        context.write(v,k);
    }
}
