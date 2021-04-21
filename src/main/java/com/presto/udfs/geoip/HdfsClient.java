package com.presto.udfs.geoip;

import com.presto.tools.UDFConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;

public class HdfsClient {

    private static FileSystem fs = null;
    private static Configuration conf = null;

    /*static {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        try {
            fs = FileSystem.get(new URI(UDFConstant.HDFS_URL), conf, UDFConstant.HDFS_USER);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }*/

    public static FSDataInputStream getFSDataInputStream(String path) throws IOException{
//        return fs.open(new Path(path));
        return null;
    }

    public static FileStatus getFileStatus(String path) throws IOException{
//        return fs.getFileStatus(new Path(path));
        return null;
    }


}
