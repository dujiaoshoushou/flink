package org.apache.flink.api.window;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: unicorn
 * Date: 2021/12/23 5:46 下午
 * Description: No Description
 */
public class TestSink {


	public static void main(String[] args) throws Exception {
		String path1 = "/Users/lujun/Downloads/test1/";
		String path2 = "/Users/lujun/Downloads/test2/";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(10L);
		/*List<User> users = Arrays.asList(new User("张三", 12),
			new User("李四", 13), new User("王五", 14));*/
		List<String> word = new ArrayList<String>();
		word.add("1");
		word.add("2");
		word.add("3");
		word.add("4");
		word.add("5");
		DataStreamSource<String
			> dataStreamSource = env.fromCollection(word);
		//writeAsText 已过时，推荐使用StreamingFileSink
		StreamingFileSink<String> sink1 = StreamingFileSink
			.forRowFormat(
				new Path(path1),
				new SimpleStringEncoder<String>("UTF-8"))
			/**
			 * 设置桶分配政策
			 * DateTimeBucketAssigner --默认的桶分配政策，默认基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH
			 * BasePathBucketAssigner ：将所有部分文件（part file）存储在基本路径中的分配器（单个全局桶）
			 */
			.withBucketAssigner(new DateTimeBucketAssigner<>())
			/**
			 * 有三种滚动政策
			 *  CheckpointRollingPolicy
			 *  DefaultRollingPolicy
			 *  OnCheckpointRollingPolicy
			 */
			.withRollingPolicy(
				/**
				 * 滚动策略决定了写出文件的状态变化过程
				 * 1. In-progress ：当前文件正在写入中
				 * 2. Pending ：当处于 In-progress 状态的文件关闭（closed）了，就变为 Pending 状态
				 * 3. Finished ：在成功的 Checkpoint 后，Pending 状态将变为 Finished 状态
				 *
				 * 观察到的现象
				 * 1.会根据本地时间和时区，先创建桶目录
				 * 2.文件名称规则：part-<subtaskIndex>-<partFileIndex>
				 * 3.在macos中默认不显示隐藏文件，需要显示隐藏文件才能看到处于In-progress和Pending状态的文件，因为文件是按照.开头命名的
				 *
				 */
				DefaultRollingPolicy.builder()
					//每隔多久（指定）时间生成一个新文件
					.withRolloverInterval(TimeUnit.SECONDS.toMillis(10))
					//数据不活动时间 每隔多久（指定）未来活动数据，则将上一段时间（无数据时间段）也生成一个文件
					.withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
					//每个文件大小
					.withMaxPartSize(1024 * 1024 * 1024)
					.build())
			/**
			 * 设置sink的前缀和后缀
			 * 文件的头和文件扩展名
			 */
			.withOutputFileConfig(OutputFileConfig
				.builder()
				.withPartPrefix("lei")
				.withPartSuffix(".txt")
				.build())
			.build();
		StreamingFileSink<String> sink2 = StreamingFileSink
			.forRowFormat(
				new Path(path2),
				new SimpleStringEncoder<String>("UTF-8"))
			/**
			 * 设置桶分配政策
			 * DateTimeBucketAssigner --默认的桶分配政策，默认基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH
			 * BasePathBucketAssigner ：将所有部分文件（part file）存储在基本路径中的分配器（单个全局桶）
			 */
			.withBucketAssigner(new DateTimeBucketAssigner<>())
			/**
			 * 有三种滚动政策
			 *  CheckpointRollingPolicy
			 *  DefaultRollingPolicy
			 *  OnCheckpointRollingPolicy
			 */
			.withRollingPolicy(
				/**
				 * 滚动策略决定了写出文件的状态变化过程
				 * 1. In-progress ：当前文件正在写入中
				 * 2. Pending ：当处于 In-progress 状态的文件关闭（closed）了，就变为 Pending 状态
				 * 3. Finished ：在成功的 Checkpoint 后，Pending 状态将变为 Finished 状态
				 *
				 * 观察到的现象
				 * 1.会根据本地时间和时区，先创建桶目录
				 * 2.文件名称规则：part-<subtaskIndex>-<partFileIndex>
				 * 3.在macos中默认不显示隐藏文件，需要显示隐藏文件才能看到处于In-progress和Pending状态的文件，因为文件是按照.开头命名的
				 *
				 */
				DefaultRollingPolicy.builder()
					//每隔多久（指定）时间生成一个新文件
					.withRolloverInterval(TimeUnit.SECONDS.toMillis(10))
					//数据不活动时间 每隔多久（指定）未来活动数据，则将上一段时间（无数据时间段）也生成一个文件
					.withInactivityInterval(TimeUnit.SECONDS.toMillis(10))
					//每个文件大小
					.withMaxPartSize(1024 * 1024 * 1024)
					.build())
			/**
			 * 设置sink的前缀和后缀
			 * 文件的头和文件扩展名
			 */
			.withOutputFileConfig(OutputFileConfig
				.builder()
				.withPartPrefix("lei")
				.withPartSuffix(".txt")
				.build())
			.build();
		dataStreamSource.addSink(sink1);
		dataStreamSource.addSink(sink2);
		dataStreamSource.print();
		env.execute();
	}


}
