package org.apache.flink.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/20 2:22 下午
 * Description: No Description
 */
public class StreamingExractTimestampsJob {
	private static final Logger log = LoggerFactory.getLogger(StreamingExractTimestampsJob.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Integer> dataStream = env.fromElements(1, 2, 3, 4, 5);
		dataStream.assignTimestamps(new TimestampExtractor<Integer>() {
			@Override
			public long extractTimestamp(Integer element, long currentTimestamp) {
				return element * currentTimestamp;
			}

			@Override
			public long extractWatermark(Integer element, long currentTimestamp) {
				return element * currentTimestamp;
			}

			@Override
			public long getCurrentWatermark() {
				return System.currentTimeMillis();
			}
		}).print();
		dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Integer>() {
			@Nullable
			@Override
			public Watermark getCurrentWatermark() {
				return new Watermark(System.currentTimeMillis());
			}

			@Override
			public long extractTimestamp(Integer element, long previousElementTimestamp) {
				return element * previousElementTimestamp;
			}
		}).print();
		dataStream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Integer>() {
			@Nullable
			@Override
			public Watermark checkAndGetNextWatermark(Integer lastElement, long extractedTimestamp) {
				return new Watermark(lastElement * extractedTimestamp);
			}

			@Override
			public long extractTimestamp(Integer element, long previousElementTimestamp) {
				return element*previousElementTimestamp;
			}
		}).print();
		env.execute();
	}
}
