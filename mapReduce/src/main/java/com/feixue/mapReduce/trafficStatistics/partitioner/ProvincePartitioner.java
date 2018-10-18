package com.feixue.mapReduce.trafficStatistics.partitioner;

import com.feixue.mapReduce.trafficStatistics.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 *
 *  定义分区规则
 *
 *      继承Partitioner抽象类
 *          实现getPartition方法
 *
 * @Author ：feixue
 * @Data : 21:10 2018/10/17
 */
public class ProvincePartitioner extends Partitioner<FlowBean,Text> {

    public static HashMap<String,Integer> provinceMap = new HashMap<>();

    static {
        provinceMap.put("134", 0);
        provinceMap.put("135", 1);
        provinceMap.put("136", 2);
        provinceMap.put("137", 3);
        provinceMap.put("138", 4);
    }

    //这里就是实际分区方法 返回就是分区编号  分区编号就决定了数据到那个分区中  part-r-00000?
    @Override
    public int getPartition(FlowBean flowBean,Text text,  int numPartitions) {

        Integer code = provinceMap.get(text.toString().substring(0,3));

        if(code != null){
            return code;
        }
        return 5;
    }
}
