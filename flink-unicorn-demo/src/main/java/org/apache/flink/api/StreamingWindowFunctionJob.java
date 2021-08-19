package org.apache.flink.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/16 10:46 上午
 * Description: No Description
 */
public class StreamingWindowFunctionJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		int port = 9003;
		String hostname = "localhost";
		String delimiter = "\n";
		//连接socket获取输入的数据
		DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

		DataStream<Tuple2<Integer, Integer>> intData = text.map(
			new MapFunction<String, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(String value) throws Exception {
					return new Tuple2<>(1, Integer.parseInt(value));
				}
			});

		intData.keyBy(0)
			.timeWindow(Time.seconds(10))
			.apply(new WindowFunction<Tuple2<Integer, Integer>, Object, Tuple, TimeWindow>() {
				@Override
				public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<Integer, Integer>> input, Collector<Object> out) throws Exception {
					int sum = 0;
					for (Tuple2 tuple2 : input) {
						System.out.println("执行apply操作："+tuple2.f0+","+tuple2.f1);
						sum += (Integer) tuple2.f1;
					}
					out.collect(new Integer(sum));
				}
			})
			.print();

		//这一行代码一定要实现，否则程序不执行
		env.execute("Socket window count");

	}
}
