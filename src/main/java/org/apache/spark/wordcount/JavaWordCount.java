package org.apache.spark.wordcount;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public final class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .config("spark.master","local")
//      .config("spark.master","local-cluster[2,1,1024]")
//      .config("spark.master","spark://192.168.146.129:7077")
                .getOrCreate();
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        FlatMapFunction<String, String> flatMapFunction = new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        };
        JavaRDD<String> words = lines.flatMap(flatMapFunction);

        PairFunction<String, String, Integer> pairFunction = new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }
        };
        Function2<Integer, Integer, Integer> function2 = new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        };
        JavaPairRDD<String, Integer> ones = words.mapToPair(
                pairFunction);

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                function2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        spark.stop();
    }
}
