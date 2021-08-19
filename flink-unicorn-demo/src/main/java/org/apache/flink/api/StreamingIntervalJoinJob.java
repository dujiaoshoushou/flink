package org.apache.flink.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/16 3:27 下午
 * Description: No Description
 */
public class StreamingIntervalJoinJob {
	private static final Logger log = LoggerFactory.getLogger(StreamingIntervalJoinJob.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		DataStreamSource<String> leftSource = env.socketTextStream("localhost", 9002,"\n");
		DataStreamSource<String> rightSource = env.socketTextStream("localhost", 9003,"\n");
		KeyedStream<Tuple2<Integer, Integer>, Tuple> leftData = leftSource.map(
			new MapFunction<String, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(String value) throws Exception {
					return new Tuple2<>(1, Integer.parseInt(value));
				}
			})
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, Integer>>() {
				@Override
				public long extractAscendingTimestamp(Tuple2<Integer, Integer> element) {
					return element.f1;
				}
			}).keyBy(0);
		KeyedStream<Tuple2<Integer, Integer>, Tuple> rightData = rightSource.map(
			new MapFunction<String, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(String value) throws Exception {
					return new Tuple2<>(1, Integer.parseInt(value));
				}
			}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Integer, Integer>>() {
			@Override
			public long extractAscendingTimestamp(Tuple2<Integer, Integer> element) {
				return element.f1;
			}
		})
			.keyBy(0);
		leftData.intervalJoin(rightData)
			.between(Time.seconds(0), Time.seconds(5))
			.upperBoundExclusive()
			.lowerBoundExclusive()
			.process(new ProcessJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Object>() {
				@Override
				public void processElement(Tuple2<Integer, Integer> left, Tuple2<Integer, Integer> right, Context ctx, Collector<Object> out) throws Exception {
					log.info("left.f0:{},left.f1:{},right.f0:{},right.f1:{}", left.f0, left.f1, right.f0, right.f1);
					left.f1 += right.f1;
					out.collect(left);
				}
			})
			.print();
		env.execute();

	}
}


