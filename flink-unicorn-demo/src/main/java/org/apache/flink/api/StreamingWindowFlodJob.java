package org.apache.flink.api;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/16 2:40 下午
 * Description: No Description
 */
public class StreamingWindowFlodJob {
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
			.fold(1, new FoldFunction<Tuple2<Integer, Integer>, Integer>() {
				@Override
				public Integer fold(Integer accumulator, Tuple2<Integer, Integer> value) throws Exception {
					System.out.println("accumulator: "+accumulator+" value1: "+ value.f0 + " value2: "+value.f1);
					return value.f1 + accumulator;
				}
			})
			.print();

		//这一行代码一定要实现，否则程序不执行
		env.execute("Socket window count");
	}
}
