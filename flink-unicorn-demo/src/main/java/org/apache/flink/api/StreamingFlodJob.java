package org.apache.flink.api;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/11 3:38 下午
 * Description: No Description
 */
public class StreamingFlodJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L))
			.keyBy(0) // 以数组的第一个元素作为key
			.fold("start", new FoldFunction<Tuple2<Long, Long>, String>() {
				@Override
				public String fold(String accumulator, Tuple2<Long, Long> value) throws Exception {
					return accumulator+value.f0+value.f1;
				}
			}).print();
		env.execute();
	}
}
