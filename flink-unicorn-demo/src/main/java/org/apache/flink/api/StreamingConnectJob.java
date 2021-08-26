package org.apache.flink.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/19 2:56 下午
 * Description: No Description
 */
public class StreamingConnectJob {
	private static final Logger log = LoggerFactory.getLogger(StreamingConnectJob.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Tuple2<String, Integer>> leftSource = env.fromElements(new Tuple2<>("unicorn", 1),
			new Tuple2<>("unicorn1", 2), new Tuple2<>("unicorn2", 3));
		DataStreamSource<Tuple2<String, Integer>> rightSource = env.fromElements(new Tuple2<>("unicorn", 1),
			new Tuple2<>("unicorn1", 2), new Tuple2<>("unicorn2", 3));
		ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connectedStreams = leftSource.connect(rightSource);
		connectedStreams.map(new CoMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Object>() {
			@Override
			public Object map1(Tuple2<String, Integer> value) throws Exception {
				return value;
			}

			@Override
			public Object map2(Tuple2<String, Integer> value) throws Exception {
				return value;
			}
		}).print();
		connectedStreams.flatMap(new CoFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Object>() {
			@Override
			public void flatMap1(Tuple2<String, Integer> value, Collector<Object> out) throws Exception {
				out.collect(value);
			}

			@Override
			public void flatMap2(Tuple2<String, Integer> value, Collector<Object> out) throws Exception {
				out.collect(value);
			}
		}).print();
		env.execute();
	}
}
