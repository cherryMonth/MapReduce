package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HBaseTemperatureImporter extends Configured implements Tool {

    /**
     *
     * 利用MapReduce 从HDFS读取信息写入 HBase
     * 内容
     */

    static class HBaseTemperatureMapper<K> extends Mapper<LongWritable, Text, K, Put> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            byte [] rowkey = Bytes.toBytes(key.get());
            Put put = new Put(rowkey);
            String [] values = value.toString().split("\t");
            put.addColumn(Bytes.toBytes("value"), Bytes.toBytes(values[0]), Bytes.toBytes(values[1]));
            context.write(null, put);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path("/output/1/part-r-00000"));

        /*
        *  TableOutputFormat.OUTPUT_TABLE　是 MapReduce　输出到 HBase的格式
        * */

        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "songjian");
        job.setMapperClass(HBaseTemperatureMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TableOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        /**
         * ToolRunner 可以运行MapReduce任务,　通常我们是使用 job来运行的
         *
         * ToolRunner 可以运行实现了 run()接口的类
         *
         */

        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new HBaseTemperatureImporter(), args);
        System.exit(exitCode);
    }
}
