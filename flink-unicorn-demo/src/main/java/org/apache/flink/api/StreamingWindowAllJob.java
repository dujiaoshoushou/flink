package org.apache.flink.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/16 9:44 上午
 * Description: No Description
 */
public class StreamingWindowAllJob {
	public static void main(String[] args) throws Exception {
		//获取需要的端口号
		int port;
		try {
			ParameterTool parameterTool = ParameterTool.fromArgs(args);
			port = parameterTool.getInt("port");
		}catch (Exception e){
			System.err.println("No port set. use default port 9003--java");
			port = 9003;
		}

		//获取flink的运行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		String hostname = "localhost";
		String delimiter = "\n";
		//连接socket获取输入的数据
		DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

		DataStream<Tuple2<Integer,Integer>> intData = text.map(
			new MapFunction<String, Tuple2<Integer,Integer>>() {
				@Override
				public Tuple2<Integer,Integer> map(String value) throws Exception {
					return new Tuple2<>(1,Integer.parseInt(value));
				}
			});

		intData
			.windowAll(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
			.reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1,
													   Tuple2<Integer, Integer> value2)
					throws Exception {
					System.out.println("执行reduce操作："+value1+","+value2);
					return new Tuple2<>(value1.f0,value1.f1+value2.f1);
				}
			}).print();

		//这一行代码一定要实现，否则程序不执行
		env.execute("Socket window count");
	}
}
