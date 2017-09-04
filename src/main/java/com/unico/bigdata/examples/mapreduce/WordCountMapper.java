package com.unico.bigdata.examples.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String currentLine = value.toString();
		String[] words = currentLine.split(" ");
//		for(String word : words){
//			Text outputKey = new Text(word);
//			context.write(outputKey, new IntWritable(1));
//		}
		Arrays.asList(words).stream().forEach(w -> {writeKeyToContext(context, w);});
	}

	private void writeKeyToContext(Context context, String w) {
		try {
			context.write( new Text(w), new IntWritable(1));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
