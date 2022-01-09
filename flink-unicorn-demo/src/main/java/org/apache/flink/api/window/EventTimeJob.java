package org.apache.flink.api.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.bean.Stock;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/12/16 9:41 下午
 * Description: No Description
 */
public class EventTimeJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		//DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
		String filePath = EventTimeJob.class.getResource("/stock.txt").getFile();
		DataStreamSource<String> dataStream = env.readTextFile(filePath);
		DataStream<Stock> stockDataStream = dataStream.map(new MapFunction<String, Stock>() {
			@Override
			public Stock map(String value) throws Exception {
				String[] split = value.split(",");
				return new Stock(split[0], Double.parseDouble(split[1]), split[2], Long.parseLong(split[3]));
			}
		});
		stockDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Stock>() {
			@Nullable
			@Override
			public Watermark getCurrentWatermark() {
				return new Watermark(2000L);
			}

			@Override
			public long extractTimestamp(Stock element, long previousElementTimestamp) {
				return element.getTimestamp();
			}
		}).keyBy((KeySelector<Stock, String>) value -> value.getId())
			.window(TumblingEventTimeWindows.of(Time.seconds(20)))
			.max("price").print("stockStream");

		env.execute();
	}
}
