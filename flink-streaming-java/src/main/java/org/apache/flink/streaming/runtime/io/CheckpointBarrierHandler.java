/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import javax.annotation.Nullable;

/**
 * The {@link CheckpointBarrierHandler} reacts to checkpoint barrier arriving from the input channels.
 * Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 */
public abstract class CheckpointBarrierHandler {

	/** The listener to be notified on complete checkpoints. */
	@Nullable
	private final AbstractInvokable toNotifyOnCheckpoint;

	public CheckpointBarrierHandler(@Nullable AbstractInvokable toNotifyOnCheckpoint) {
		this.toNotifyOnCheckpoint = toNotifyOnCheckpoint;
	}

	public abstract void releaseBlocksAndResetBarriers();

	/**
	 * Checks whether the channel with the given index is blocked.
	 *
	 * @param channelIndex The channel index to check.
	 * @return True if the channel is blocked, false if not.
	 */
	public abstract boolean isBlocked(int channelIndex);

	/**
	 * @return true if some blocked data should be unblocked/rolled over.
	 */
	public abstract boolean processBarrier(CheckpointBarrier receivedBarrier, int channelIndex, long bufferedBytes) throws Exception;

	/**
	 * @return true if some blocked data should be unblocked/rolled over.
	 */
	public abstract boolean processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception;

	/**
	 * @return true if some blocked data should be unblocked/rolled over.
	 */
	public abstract boolean processEndOfPartition() throws Exception;

	public abstract long getLatestCheckpointId();

	public abstract long getAlignmentDurationNanos();

	public abstract void checkpointSizeLimitExceeded(long maxBufferedBytes) throws Exception;

	/**
	 * 1. 判断toNotifyOnCheckpoint不为空，这里的toNotifyOnCheckpoint实例实际上就是AbstractInvokable实现类，在AbstractInvokable中提供了触发Checkpoint操作的相关方法。
	 *    StreamTask是唯一实现了Checkpoint方法的子类，即只有StreamTask才能触发当前Task实例中的Checkpoint操作。
	 * 2. 创建CheckpointMetaData和CheckpointMetrics实例，CheckpointMetaData用于存储Checkpoint的元信息，CheckpointMetrics用于记录和监控Checkpoint监控指标。
	 * 3. 调用toNotifyOnCheckpoint.triggerCheckpointOnBarrier()方法触发StreamTask中算子的Checkpoint操作。
	 * 4. triggerCheckpointOnBarrier()方法基本上和CheckpointCoordinator触发数据源节点的Checkpoint操作执行过程一致。
	 */
	protected void notifyCheckpoint(CheckpointBarrier checkpointBarrier, long bufferedBytes, long alignmentDurationNanos) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			// 创建CheckpointMetaData对象用于存储Meta信息
			CheckpointMetaData checkpointMetaData =
				new CheckpointMetaData(checkpointBarrier.getId(), checkpointBarrier.getTimestamp());
			// 创建CheckpointMetrics对象用于记录监控指标
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
				.setBytesBufferedInAlignment(bufferedBytes)
				.setAlignmentDurationNanos(alignmentDurationNanos);
			// 调用toNotifyOnCheckpoint.triggerCheckpointOnBarrier()方法触发Checkpoint操作
			toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
				checkpointMetaData,
				checkpointBarrier.getCheckpointOptions(),
				checkpointMetrics);
		}
	}

	protected void notifyAbortOnCancellationBarrier(long checkpointId) throws Exception {
		notifyAbort(checkpointId,
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
	}

	protected void notifyAbort(long checkpointId, CheckpointException cause) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
		}
	}
}
