package com.feixue.mapReudce.trafficStatistics;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 *  要使用自定义的数据类型要实现序列化接口 Writable
 *
 *    1 实现get set 方法
 *    2 实现构造函数 有参 无参
 *
 *
 *
 * @Author ：feixue
 * @Data : 19:05 2018/10/17
 */
public class FlowBean implements WritableComparable<FlowBean> {

    // 上行流量
    private long upFlow;
    // 下行流量
    private long downFlow;

    private long  sumFlow;

    public FlowBean() {
    }

    public FlowBean(long upFlow, long downFlow, long sumFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }


    public  void set(long upFlow,long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }


    // 序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);

        out.writeLong(downFlow);

        out.writeLong(sumFlow);

    }


    // 反序列方法
    // 注意序列化的循序，先序列化，先反序列化
    @Override
    public void readFields(DataInput in) throws IOException {

        this.upFlow = in.readLong();

        this.downFlow = in.readLong();

        this.sumFlow = in.readLong();


    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    // 比较大小的方法
    @Override
    public int compareTo(FlowBean o) {
        // 按照总流量排序的倒序排序
        //return this.sumFlow > o.sumFlow ? -1 : 1;
        // 正序
        return this.sumFlow > o.sumFlow ? 1 : -1;
    }
}
