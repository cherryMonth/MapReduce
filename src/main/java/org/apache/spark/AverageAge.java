package org.apache.spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AverageAge {
    private static final String SPACE = " ";

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("AverageAge");
        sparkConf.set("spark.driver.memory ", "200m");
        sparkConf.set("spark.master", "local");


        // 读取文件
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> dataFile = sc.textFile("hdfs://hadoop:9000/input/ages.txt");

        // 数据分片并取第二个数
        JavaRDD<String> ageData = dataFile.flatMap(s -> Arrays.asList(s.split(SPACE)[1]).iterator());

        // 求出所有年龄个数。
        long count = ageData.count();

        // 转换数据类型
        JavaRDD<Long> ageDataLong = ageData.map(s -> Long.parseLong(String.valueOf(s)));

        // 求出年龄的和
        Long totalAge = ageDataLong.reduce((x, y) -> x + y);

        // 计算平均值
        Double avgAge = totalAge.doubleValue() / count;

        // 输出结果
        System.out.println("Total Age:" + totalAge + ";    Number of People:" + count);
        System.out.println("Average Age is " + avgAge);

        sc.close();
    }
}
