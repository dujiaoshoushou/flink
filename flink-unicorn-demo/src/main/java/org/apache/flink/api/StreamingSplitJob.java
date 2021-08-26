package org.apache.flink.api;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/19 9:43 下午
 * Description: No Description
 */
public class StreamingSplitJob {
	private static final Logger log = LoggerFactory.getLogger(StreamingSplitJob.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 3, 3, 4);
		SplitStream<Integer> splitStream = dataStreamSource.split(new OutputSelector<Integer>() {
			@Override
			public Iterable<String> select(Integer value) {
				List<String> output = new ArrayList<>();
				if (value % 2 == 0) {
					output.add("even");
				} else {
					output.add("odd");
				}
				return output;
			}
		});
		//splitStream.print();

		splitStream.select("even").print();
//		splitStream.select("odd").print();
		env.execute();
	}
}
