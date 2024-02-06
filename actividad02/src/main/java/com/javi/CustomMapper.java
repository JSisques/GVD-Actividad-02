package com.javi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper
 */
public class CustomMapper extends MapReduceBase implements Mapper {

    private final static IntWritable one = new IntWritable(1);
    private String[] lines;

    @Override
    public void map(Object key, Object value, OutputCollector output, Reporter reporter) throws IOException {
        String content = value.toString();
        lines = content.split("\n");

        for (String line : lines) {
            int weight = Integer.valueOf(line.split(",")[2]);
            output.collect(weight, one);
        }
    }

}