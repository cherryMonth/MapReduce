//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
//import org.apache.hadoop.hbase.io.*;
//import org.apache.hadoop.hbase.mapred.TableMap;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import java.util.Map;
//
//public class RowCounter extends Configured implements Tool{
//
//        static final String NAME = "rowcounter";
//
//        static class RowCounterMapper implements TableMap<ImmutableBytesWritable, RowResult>{
//            private static enum Counters {ROWS};
//
//            public void map(ImmutableBytesWritable row, Result result, OutputCollector<ImmutableBytesWritable, Result> out, Reporter reporter){
//                boolean content = false;
//                for(Map.Entry<byte [], Cell> e: result.)
//            }
//
//        }
//}
