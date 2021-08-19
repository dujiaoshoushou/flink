package org.apache.flink.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/11 2:23 下午
 * Description: No Description
 */
public class StreamingKeyByJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L))
			.keyBy(0) // 以数组的第一个元素作为key
			.map((MapFunction<Tuple2<Long, Long>, String>) longLongTuple2 -> "key:" + (longLongTuple2.f0*2) + ",value:" + longLongTuple2.f1)
			.print();

		env.execute("execute");
	}
}
