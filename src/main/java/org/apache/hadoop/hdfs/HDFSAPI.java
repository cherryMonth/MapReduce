package org.apache.hadoop.hdfs;

/***
 *  HDFS的CRD
 *  HDFS不支持修改
 */

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HDFSAPI {

    // 判断路径是否存在
    public static boolean test(Configuration conf, String path) {
        try (FileSystem fs = FileSystem.get(conf)) {
            return fs.exists(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    // 复制文件到指定路径，若路径已经存在，则进行覆盖

    public static void copyFromLocalFile(Configuration conf, String localFilePath, String remoteFilePath) {
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        try (FileSystem fs = FileSystem.get(conf)) {
            fs.copyFromLocalFile(false, true, localPath, remotePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 追加文件内容

    public static void appendToFile(Configuration conf, String localFilePath, String remoteFilePath) {
        Path remotePath = new Path(remoteFilePath);
        try (FileSystem fs = FileSystem.get(conf)) {
            FileInputStream in = new FileInputStream(localFilePath);
            FSDataOutputStream out = fs.append(remotePath);
            byte[] data = new byte[1024];
            int read = -1;
            while ((read = in.read(data)) > 0) {
                out.write(data, 0, read);
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        // args[0] 输入路径
        // args[1] 输出路径
        // args[2] 文件写入参数 覆盖或者追加

        Configuration conf = new Configuration();
        try {
            boolean fileExists = false;
            if (HDFSAPI.test(conf, args[1])) {
                fileExists = true;
                System.out.println(args[1] + "已存在!");
            } else {
                System.out.println(args[1] + "不存在!");
            }

            if (fileExists) {
                HDFSAPI.copyFromLocalFile(conf, args[0], args[1]);
                System.out.println("已上传");
            } else if (args[2].equals("overwrite")) {
                HDFSAPI.copyFromLocalFile(conf, args[0], args[1]);
                System.out.println("已覆盖");
            } else {
                HDFSAPI.appendToFile(conf, args[0], args[1]);
                System.out.println("已追加");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}