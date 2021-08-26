package org.apache.flink.api;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/20 2:37 下午
 * Description: No Description
 */
public class StreamingProjectJob {
	private static final Logger log = LoggerFactory.getLogger(StreamingProjectJob.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(new Tuple2<>("unicorn", 1),
			new Tuple2<>("unicorn1", 2), new Tuple2<>("unicorn2", 3));
		dataStream.project(1).print();
		env.execute();
	}
}
