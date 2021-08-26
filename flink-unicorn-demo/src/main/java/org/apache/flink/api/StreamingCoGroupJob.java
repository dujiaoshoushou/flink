package org.apache.flink.api;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/8/19 2:34 下午
 * Description: No Description
 */
public class StreamingCoGroupJob {

	private static final Logger log = LoggerFactory.getLogger(StreamingCoGroupJob.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		DataStream<Tuple2<String, Integer>> leftSource = env.fromElements(new Tuple2<String, Integer>("unicorn", 1),
			new Tuple2<String, Integer>("unicorn1", 2), new Tuple2<String, Integer>("unicorn2", 3));
		DataStream<Tuple2<String, Integer>> rightSource = env.fromElements(new Tuple2<String, Integer>("unicorn", 1),
			new Tuple2<String, Integer>("unicorn1", 2), new Tuple2<String, Integer>("unicorn2", 3));
		leftSource.coGroup(rightSource)
			.where(new KeySelector<Tuple2<String, Integer>, Object>() {
				@Override
				public Object getKey(Tuple2<String, Integer> value) throws Exception {
					log.info("first.f0:{},first.f1:{}", value.f0, value.f1);
					return value.f0;
				}
			}).equalTo(new KeySelector<Tuple2<String, Integer>, Object>() {
			@Override
			public Object getKey(Tuple2<String, Integer> value) throws Exception {
				log.info("second.f0:{},second.f1:{}", value.f0, value.f1);
				return value.f0;
			}
		})
			.window(TumblingEventTimeWindows.of(Time.seconds(3)))
			.apply(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Object>() {
				@Override
				public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Object> out) throws Exception {
					first.forEach(o -> {
						log.info("o.f0:{},o.f1:{}", o.f0, o.f1);
						o.f1 += 1;
					});
					second.forEach(o -> {
						log.info("second.f0:{},second.f1:{}", o.f0, o.f1);
						o.f1 += 1;
					});
					out.collect(first);
					out.collect(second);
				}
			}).print();
		env.execute();
	}
}
