/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Disposable;

import java.io.Serializable;

/**
 * Basic interface for stream operators. Implementers would implement one of
 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator} or
 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator} to create operators
 * that process elements.
 *
 * <p>The class {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator}
 * offers default implementation for the lifecycle and properties methods.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with
 * methods on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@PublicEvolving
public interface StreamOperator<OUT> extends CheckpointListener, KeyContext, Disposable, Serializable {

	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------

	/**
	 * This method is called immediately before any elements are processed, it should contain the
	 * operator's initialization logic.
	 *
	 * @implSpec In case of recovery, this method needs to ensure that all recovered data is processed before passing
	 * back control, so that the order of elements is ensured during the recovery of an operator chain (operators
	 * are opened from the tail operator to the head operator).
	 *
	 * @throws java.lang.Exception An exception in this method causes the operator to fail.
	 *
	 * 定义当前Operator的初始化方法，在数据元素正式接入Operator运算之前，Task会调用StreamOperator.open()方法对算子进行初始化。
	 */
	void open() throws Exception;

	/**
	 * This method is called after all records have been added to the operators via the methods
	 * {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator#processElement(StreamRecord)}, or
	 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement1(StreamRecord)} and
	 * {@link org.apache.flink.streaming.api.operators.TwoInputStreamOperator#processElement2(StreamRecord)}.
	 *
	 * <p>The method is expected to flush all remaining buffered data. Exceptions during this
	 * flushing of buffered should be propagated, in order to cause the operation to be recognized
	 * as failed, because the last data items are not processed properly.
	 *
	 * @throws java.lang.Exception An exception in this method causes the operator to fail.
	 * 当所有的数据元素都添加到当前Operator时，就会调用该方法刷新所有剩余的缓冲数据，保证算子中所有数据被正确处理。
	 */
	void close() throws Exception;

	/**
	 * This method is called at the very end of the operator's life, both in the case of a successful
	 * completion of the operation, and in the case of a failure and canceling.
	 *
	 * <p>This method is expected to make a thorough effort to release all resources
	 * that the operator has acquired.
	 */
	@Override
	void dispose() throws Exception;

	// ------------------------------------------------------------------------
	//  state snapshots
	// ------------------------------------------------------------------------

	/**
	 * This method is called when the operator should do a snapshot, before it emits its
	 * own checkpoint barrier.
	 *
	 * <p>This method is intended not for any actual state persistence, but only for emitting some
	 * data before emitting the checkpoint barrier. Operators that maintain some small transient state
	 * that is inefficient to checkpoint (especially when it would need to be checkpointed in a
	 * re-scalable way) but can simply be sent downstream before the checkpoint. An example are
	 * opportunistic pre-aggregation operators, which have small the pre-aggregation state that is
	 * frequently flushed downstream.
	 *
	 * <p><b>Important:</b> This method should not be used for any actual state snapshot logic, because
	 * it will inherently be within the synchronous part of the operator's checkpoint. If heavy work is done
	 * within this method, it will affect latency and downstream checkpoint alignments.
	 *
	 * @param checkpointId The ID of the checkpoint.
	 * @throws Exception Throwing an exception here causes the operator to fail and go into recovery.
	 * 在StreamOperator正式执行checkpoint操作之前会调用该方法，目前仅有MapBundleOperator算子中使用该方法。
	 */
	void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

	/**
	 * Called to draw a state snapshot from the operator.
	 *
	 * @return a runnable future to the state handle that points to the snapshotted state. For synchronous implementations,
	 * the runnable might already be finished.
	 *
	 * @throws Exception exception that happened during snapshotting.
	 * 当SubTask执行checkpoint操作时会调用该方法，用于触发该Operatior中状态数据的快照操作。
	 */
	OperatorSnapshotFutures snapshotState(
		long checkpointId,
		long timestamp,
		CheckpointOptions checkpointOptions,
		CheckpointStreamFactory storageLocation) throws Exception;

	/**
	 * Provides a context to initialize all state in the operator.
	 * 当算子启动或重启时，调用该方法初始化状态数据，当恢复作用任务时，算子会从检查点（checkpoint）持久化的数据中恢复状态数据。
	 */
	void initializeState() throws Exception;

	// ------------------------------------------------------------------------
	//  miscellaneous
	// ------------------------------------------------------------------------

	void setKeyContextElement1(StreamRecord<?> record) throws Exception;

	void setKeyContextElement2(StreamRecord<?> record) throws Exception;

	ChainingStrategy getChainingStrategy();

	void setChainingStrategy(ChainingStrategy strategy);

	MetricGroup getMetricGroup();

	OperatorID getOperatorID();
}
