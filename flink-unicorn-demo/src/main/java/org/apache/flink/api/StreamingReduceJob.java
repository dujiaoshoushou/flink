package org.apache.flink.api;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/11 3:24 下午
 * Description: No Description
 */
public class StreamingReduceJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
		env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L))
			.keyBy(0) // 以数组的第一个元素作为key
			.reduce((ReduceFunction<Tuple2<Long, Long>>) (t2, t1) -> new Tuple2<>(t1.f0, t2.f1 + t1.f1)) // value做累加
			.print();

		env.execute("execute");
	}
}
