package com.javi;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * Driver
 */
public class Driver {

    private CustomMapper mapper;
    private CustomReducer reducer;

    public Driver() {
        mapper = new CustomMapper();
        reducer = new CustomReducer();
    }

    public void mapReduce(String[] args) {
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("Contador de pesos");

        conf.setOutputKeyClass(Integer.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(CustomMapper.class);
        conf.setCombinerClass(CustomReducer.class);
        conf.setReducerClass(CustomReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        try {
            JobClient.runJob(conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}