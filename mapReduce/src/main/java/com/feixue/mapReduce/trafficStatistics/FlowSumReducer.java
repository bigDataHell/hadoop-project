package com.feixue.mapReduce.trafficStatistics;


import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 *
 *  <手机号1，bean><手机号2，bean><手机号1，bean><手机号2，bean>
 *
 *      <手机号1，bean><手机号1，bean>
 *      <手机号2，bean><手机号2，bean>
 *
 * @Author ：feixue
 * @Data : 19:34 2018/10/17
 */
public class FlowSumReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    private FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long upFlowCount = 0;
        long downFlowCount = 0;

        for (FlowBean bean : values) {
            upFlowCount += bean.getUpFlow();
            downFlowCount += bean.getDownFlow();
        }

        v.set(upFlowCount, downFlowCount);

        context.write(key,v);
    }
}
