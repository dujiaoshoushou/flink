package org.apache.flink.api;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/19 2:15 下午
 * Description: No Description
 */
public class StreamingWindowJoinJob {
	private static final Logger log = LoggerFactory.getLogger(StreamingWindowJoinJob.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setParallelism(1);
		DataStreamSource<String> leftSouce = env.socketTextStream("localhost", 9002, "\n");
		DataStreamSource<String> rightSouce = env.socketTextStream("localhost", 9003, "\n");
		DataStream<Tuple2<Integer, Integer>> leftData = leftSouce.map(
			new MapFunction<String, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(String value) throws Exception {
					return new Tuple2<>(1, Integer.parseInt(value));
				}
			});
		DataStream<Tuple2<Integer, Integer>> rightData = rightSouce.map(
			new MapFunction<String, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(String value) throws Exception {
					return new Tuple2<>(1, Integer.parseInt(value));
				}
			});
		leftData.join(rightData)
			.where(new KeySelector<Tuple2<Integer, Integer>, Object>() {
				@Override
				public Object getKey(Tuple2<Integer, Integer> value) throws Exception {
					return value;
				}
			}).equalTo(new KeySelector<Tuple2<Integer, Integer>, Object>() {
			@Override
			public Object getKey(Tuple2<Integer, Integer> value) throws Exception {
				return value;
			}
		}).window(TumblingEventTimeWindows.of(Time.seconds(3)))
			.apply(new JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Object>() {
				@Override
				public Object join(Tuple2<Integer, Integer> first, Tuple2<Integer, Integer> second) throws Exception {
					first.f1 += second.f1;
					return first;
				}
			}).print();
		env.execute();
	}
}
