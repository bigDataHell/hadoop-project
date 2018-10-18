package com.feixue.mapReduce.topN;

import com.feixue.mapReduce.trafficStatistics.sort.FlowSumSort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;


/**
 *
 *   ASC : 正序 从小到大
 *   DESC :倒序 从大到小
 *
 *
如果将```compareTo()```返回值写死为0，元素值每次比较，都认为是相同的元素，这时就不再向TreeSet中插入除第一个外的新元素。所以TreeSet中就只存在插入的第一个元素。
如果将```compareTo()```返回值写死为1，元素值每次比较，都认为新插入的元素比上一个元素大，于是二叉树存储时，会存在根的右侧，读取时就是正序排列的。
如果将```compareTo()```返回值写死为-1，元素值每次比较，都认为新插入的元素比上一个元素小，于是二叉树存储时，会存在根的左侧，读取时就是倒序序排列的
 *
 * @Author ：feixue
 * @Data : 15:15 2018/10/18
 */
public class TopNJob {

    /**
     * 驱动程序，使用Job工具类来生成job
     */
    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 3) {
            System.err.println("Parameter Errors! Usages:<inputpath> <outputpath> <topN>");
            System.exit(-1);
        }

        // 向conf中传入参数
        // 在MapReduce中，因为计算是分散到每个节点上进行的
        // 也就是将我们的Maper和Reducer也是分散到每个节点进行的
        // 所以不能在TopNJob中设置一个全局变量来对N进行设置（虽然在本地运行时是没有问题的，但在集群运行时会有问题）
        // 因此MapReduce提供了在Configuration对象中设置参数的方法
        // 通过在Configuration对象中设置某些参数，可以保证每个节点的Mapper和Reducer都能够读取到N
        Configuration conf = new Configuration();
        conf.set("topN", args[2]);

        Job job = Job.getInstance(conf);

        //指定我这个 job 所在的 jar包位置
        job.setJarByClass(FlowSumSort.class);

        //指定我们使用的Mapper是那个类  reducer是哪个类
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        // 设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);


//        FileInputFormat.setInputPaths(job, new Path("D:\\DataSource\\topN"));
//        // 指定处理完成之后的结果所保存的位置
//        FileOutputFormat.setOutputPath(job, new Path("D:\\flowsum\\outputsortASCTopN"));

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // ReduceTask必须设置为1
        job.setNumReduceTasks(1);

        // 向 yarn 集群提交这个 job
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }

    /**
     * Mapper，因为Block中的每一个split都会交由一个Mapper Task来进行处理，对于TopN问题，可以考虑每一个Mapper Task的输出
     * 可以为这个split中的前N个值，最后每个数据到达Reducer的时候，就可以大大减少原来需要比较的数据量，因为在Reducer处理之前
     * Map Task已经帮我们把的数据量大大减少了，比如，在MapReduce中，默认情况下一个Block就为一个split，当然这个是可以设置的
     * 而一个Block为128M，显然128M能够存储的文本文件也是相当多的，假设现在我的数据有10个Block，即1280MB的数据，如果要求Top10
     * 的问题，此时，这些数据需要10个Mapper Task来进行处理，那么在每个Mapper Task中先求出前10个数，最后这10个数再交由Reducer来进行处理
     * 也就是说，在我们的这个案例中，Reducer需要处理排序的数有100个，显然经过Map处理之后，Reducer的压力就大大减少了。
     * 那么如何实现每个Mapper Task中都只输出10个数呢？这时可以使用一个set来缓存数据，从而达到先缓存10个数的目的，详细可以参考下面的代码。
     */
    public  static class TopNMapper extends Mapper<LongWritable,Text,IntWritable,NullWritable>{

        IntWritable intWritable = new IntWritable();


        TreeSet<Integer> cachedTopN = null;
        Integer N = null;

        /**
         * 每个Mapper Task执行前都会先执行setup函数
         * map函数是每行执行一次
         */

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // TreeSet定义的排序规则为倒序，后面做数据的处理时只需要pollLast最后一个即可将
            // TreeSet中较小的数去掉
            cachedTopN = new TreeSet<>(new Comparator<Integer>() {
                // -1 倒序 1 正序 0
                @Override
                public int compare(Integer o1, Integer o2) {
                    int ret = 0;
                    if(o1> o2){
                        return 1;
                    }else if (o1 < o2){
                        return -1;
                    }
                    return ret;
                }
            });
            // 拿到传入参数时的topN中的N值
            N = Integer.valueOf(context.getConfiguration().get("topN"));
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[]  fields =value.toString().split(",");

            if( fields == null ||  fields.length < 3 ){
                return;
            }

            Integer payment = null;

            try {
                payment = Integer.valueOf(fields[2]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return;
            }

            // 将数字写入到TreeSet当中
            cachedTopN.add(payment);
            // 判断cachedTopN中的元素个数是否已经达到N个，如果已经达到N个，则去掉最后一个
            if (cachedTopN.size() > N) {
                cachedTopN.pollLast();
            }

        }

        /**
         * 每个Mapper Task执行结束后才会执行cleanup函数
         * 将map函数筛选出来的前N个数写入到context中作为输出
         * 将
         * map函数是每行执行一次
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer num : cachedTopN) {
                intWritable.set(num);
                context.write(intWritable, NullWritable.get());
            }
        }
    }


    /**
     * Reducer，将Mapper Task输出的数据排序后再输出
     * 处理思路与Mapper是类似的
     */
    public static class TopNReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {

        IntWritable k = new IntWritable();
        IntWritable v = new IntWritable();
        TreeSet<Integer> cachedTopN = null;
        Integer N = null;

        /**
         * 初始化一个TreeSet
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // TreeSet定义的排序规则为倒序，后面做数据的处理时只需要pollLast最后一个即可将
            // TreeSet中较小的数去掉
            cachedTopN = new TreeSet<Integer>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    int ret = 0;
                    if (o1 > o2) {
                        ret = -1;
                    } else if (o1 < o2) {
                        ret = 1;
                    }
                    return ret;
                }
            });
            // 拿到传入参数时的topN中的N值
            N = Integer.valueOf(context.getConfiguration().get("topN"));
        }

        /**
         * 筛选Reducer Task中的前10个数
         */
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

            cachedTopN.add(Integer.valueOf(key.toString()));
            // 判断cachedTopN中的元素个数是否已经达到N个，如果已经达到N个，则去掉最后一个
            if (cachedTopN.size() > N) {
                cachedTopN.pollLast();
            }
        }

        /**
         * 将reduce函数筛选出来的前N个数写入到context中作为输出
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int index = 1;
            for(Integer num : cachedTopN) {
                k.set(index);
                v.set(num);
                context.write(k, v);
                index++;
            }
        }
    }


}


