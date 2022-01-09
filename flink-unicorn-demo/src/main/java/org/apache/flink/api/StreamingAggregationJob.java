package org.apache.flink.api;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/11 3:45 下午
 * Description: No Description
 */
public class StreamingAggregationJob {
	private static final Logger log = LoggerFactory.getLogger(StreamingAggregationJob.class);

	public static void main(String[] args) throws Exception {
		log.info("dfddd");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L))
			.keyBy(0) // 以数组的第一个元素作为key
			.min(0)
			.print();
		env.execute();
	}
}
