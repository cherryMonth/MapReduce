package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RowCounter extends Configured implements Tool {

    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
        public static enum CounterSongjian { songjian } // mapreduce　计数器 必须要是枚举类型

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) {
            context.getCounter(CounterSongjian.songjian).increment(1);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String tableName = args[0];
        Scan scan = new Scan();

        // scan 可以设置过滤器　过滤掉不需要的行
        // FirstKeyOnlyFilter　是默认的第一个KV
        scan.setFilter(new FirstKeyOnlyFilter());

        Job job = new Job(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());
        TableMapReduceUtil.initTableMapperJob(tableName, scan,
                RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String [] arg = {"test"};
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new RowCounter(), arg);
        System.exit(exitCode);
    }
}