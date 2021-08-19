package org.apache.flink.api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/11 1:59 下午
 * Description: No Description
 */
public class StreamingFilterJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
		List<Integer> list = new ArrayList<>();
		list.add(1);
		list.add(2);
		list.add(3);
		list.add(4);
		DataStream<Integer> dataStream = evn.fromCollection(list);
		dataStream = dataStream.filter(new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer value) throws Exception {
				if (value >= 2) {
					return true;
				}
				return false;
			}
		});
		dataStream.print();
		evn.execute();
	}
}
