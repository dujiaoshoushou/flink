package org.apache.flink.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/16 3:10 下午
 * Description: No Description
 */
public class StreamingUnionJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<Integer, Integer>> dataStream1 = env.fromElements(Tuple2.of(1, 2), Tuple2.of(3, 3));
		DataStream<Tuple2<Integer, Integer>> dataStream2 = env.fromElements(Tuple2.of(3, 3), Tuple2.of(2, 3));
		dataStream1.union(dataStream2).print();
		env.execute();
	}
}
