package org.apache.flink.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/16 11:07 上午
 * Description: No Description
 */
public class StreamingAllWindowFunctionJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

		DataStream<Tuple2<String, Integer>> window1 = source
			.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
			.apply(new AllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void apply(
					TimeWindow window,
					Iterable<Tuple2<String, Integer>> values,
					Collector<Tuple2<String, Integer>> out) throws Exception {
					for (Tuple2<String, Integer> in : values) {
						System.out.println("value1：" + in.f0 + "value2：" + in.f1);
						out.collect(in);
					}
				}
			});
		window1.print();
		env.execute("dd");
	}
}
