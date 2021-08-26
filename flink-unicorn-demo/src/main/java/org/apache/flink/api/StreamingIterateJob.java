package org.apache.flink.api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/19 9:51 下午
 * Description: No Description
 */
public class StreamingIterateJob {
	private static final Logger log = LoggerFactory.getLogger(StreamingIterateJob.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5, 6);
		IterativeStream<Integer> iteration = dataStream.iterate();
		DataStream<Integer> iterationBody = iteration.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				return value * 10;
			}
		});
		DataStream<Integer> feedback = iterationBody.filter(new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer value) throws Exception {
				return value > 0;
			}
		});
		feedback.print();
		iteration.closeWith(feedback);
		DataStream<Integer> output = iterationBody.filter(new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer value) throws Exception {
				return value < 0;
			}
		});
		output.print();
		env.execute();
	}
}
