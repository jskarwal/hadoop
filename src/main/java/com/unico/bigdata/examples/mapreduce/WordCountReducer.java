package com.unico.bigdata.examples.mapreduce;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        Sum sum = new Sum();
        StreamSupport.stream(values.spliterator(), false).forEach(count -> {
            sum.add(count.get());
        });
        IntWritable outputValue = new IntWritable(sum.getSum());
        context.write(key, outputValue);

    }

    private class Sum {
        private int sum = 0;

        public void add(int a) {
            sum = sum + a;
        }

        public int getSum() {
            return sum;
        }

    }

}
