package com.bigdata.hadoop_offline.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


public class FileUtils {

    public static void deleteOutput(Configuration configuration, String output) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(output);
        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }

        fileSystem.close();

    }
}
