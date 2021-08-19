package org.apache.flink.api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/11 1:42 下午
 * Description: No Description
 */
public class StreamingFlatMapJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env =
			StreamExecutionEnvironment.getExecutionEnvironment();
		List<Integer> tests = new ArrayList<>();
		tests.add(1);
		tests.add(2);
		tests.add(3);

		DataStream<Integer> dataStream = env.fromCollection(tests);
		dataStream = dataStream.flatMap(new FlatMapFunction<Integer, Integer>() {
			@Override
			public void flatMap(Integer value, Collector<Integer> out) throws Exception {
				out.collect(value * 2);
			}
		});

		dataStream.print();
		env.execute();

	}
}
