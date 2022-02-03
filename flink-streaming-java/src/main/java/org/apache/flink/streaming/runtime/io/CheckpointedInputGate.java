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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.PullingAsyncDataInput;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link CheckpointedInputGate} uses {@link CheckpointBarrierHandler} to handle incoming
 * {@link CheckpointBarrier} from the {@link InputGate}.
 */
@Internal
public class CheckpointedInputGate implements PullingAsyncDataInput<BufferOrEvent> {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointedInputGate.class);

	private final CheckpointBarrierHandler barrierHandler;

	/** The gate that the buffer draws its input from. */
	private final InputGate inputGate;

	private final int channelIndexOffset;

	private final BufferStorage bufferStorage;

	/** Flag to indicate whether we have drawn all available input. */
	private boolean endOfInputGate;

	/** Indicate end of the input. Set to true after encountering {@link #endOfInputGate} and depleting
	 * {@link #bufferStorage}. */
	private boolean isFinished;

	public CheckpointedInputGate(
			InputGate inputGate,
			BufferStorage bufferStorage,
			String taskName,
			@Nullable AbstractInvokable toNotifyOnCheckpoint) {
		this(
			inputGate,
			bufferStorage,
			new CheckpointBarrierAligner(
				inputGate.getNumberOfInputChannels(),
				taskName,
				toNotifyOnCheckpoint)
		);
	}

	public CheckpointedInputGate(
			InputGate inputGate,
			BufferStorage bufferStorage,
			CheckpointBarrierHandler barrierHandler) {
		this(inputGate, bufferStorage, barrierHandler, 0);
	}

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>The aligner will allow only alignments that buffer up to the given number of bytes.
	 * When that number is exceeded, it will stop the alignment and notify the task that the
	 * checkpoint has been cancelled.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param bufferStorage The storage to hold the buffers and events for blocked channels.
	 * @param barrierHandler Handler that controls which channels are blocked.
	 * @param channelIndexOffset Optional offset added to channelIndex returned from the inputGate
	 *                           before passing it to the barrierHandler.
	 */
	public CheckpointedInputGate(
			InputGate inputGate,
			BufferStorage bufferStorage,
			CheckpointBarrierHandler barrierHandler,
			int channelIndexOffset) {
		this.inputGate = inputGate;
		this.channelIndexOffset = channelIndexOffset;
		this.bufferStorage = checkNotNull(bufferStorage);
		this.barrierHandler = barrierHandler;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (bufferStorage.isEmpty()) {
			return inputGate.getAvailableFuture();
		}
		return AVAILABLE;
	}

	/**
	 * 1. 从Optional<BufferOrEvent> next中获取BufferOrEvent类型数据，BufferOrEvent即定义了Buffer数据也定义了Event。CheckpointedInputGate中主要处理
	 *    CheckpointBarrier事件，其他类型事件和数据全部传递给算子进行后续处理。
	 * 2. 如果当前InputChannel被barrierHandler对象锁定，则将所有的BufferOrEvent数据本地缓存，直到InputChannel的锁被打开。barrierHandler会等所有InputChannel的
	 *    CheckpointBarrier事件消息全部到达节点后，才继续处理该Task实例的Buffer数据，保证数据计算结果的正确性。
	 * 3. 在Buffer数据的处理过程中，如果Buffer缓冲区被填满，会进行清理操作和BarrierReset操作。
	 * 4. 如果BufferOrEvent的消息类型为Buffer，则直接返回next;如果是CheckpointBarrier类型，则调用barrierHandler.processBarrier()方法处理接入的CheckpointBarrier事件，
	 *    最终根据CheckpointBarrier对齐情况选择是否触发当前节点的Checkpoint操作。
	 * 5. 如果接收到的是CancelCheckpointMarker事件，则调用processCancellationBarrier()方法进行处理，取消本次Checkpoint操作。
	 * 6. 如果接收到的是EndOfPartitionEvent事件，表示上游Partition中的数据已经消费完毕，此时调用barrierHandler.processEndOfPartition()方法进行处理，最后清理缓冲区中的Buffer数据。
	 */
	@Override
	public Optional<BufferOrEvent> pollNext() throws Exception {
		while (true) {
			// process buffered BufferOrEvents before grabbing new ones

			Optional<BufferOrEvent> next;
			if (bufferStorage.isEmpty()) {
				next = inputGate.pollNext();
			}
			else {
				// TODO: FLINK-12536 for non credit-based flow control, getNext method is blocking
				next = bufferStorage.pollNext();
				if (!next.isPresent()) {
					return pollNext();
				}
			}

			if (!next.isPresent()) {
				return handleEmptyBuffer();
			}
            // 获取BufferOrEvent数据
			BufferOrEvent bufferOrEvent = next.get();
			if (barrierHandler.isBlocked(offsetChannelIndex(bufferOrEvent.getChannelIndex()))) {
				// if the channel is blocked, we just store the BufferOrEvent
				// 如果当前channel被barrierHandler对象锁定，则将bufferOrEvent数据先缓存下来。
				bufferStorage.add(bufferOrEvent);
				// 如果缓存区被填满，则进行清理操作和Barrier Reset操作
				if (bufferStorage.isFull()) {
					barrierHandler.checkpointSizeLimitExceeded(bufferStorage.getMaxBufferedBytes());
					bufferStorage.rollOver();
				}
			}
			else if (bufferOrEvent.isBuffer()) {
				// 如果是业务数据则直接返回，留个算子处理
				return next;
			}
			else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
				// 如果是CheckpointBarrier类型的事件，则对接入的Barrier进行处理
				CheckpointBarrier checkpointBarrier = (CheckpointBarrier) bufferOrEvent.getEvent();
				if (!endOfInputGate) {
					// process barriers only if there is a chance of the checkpoint completing
					// 根据算子的对齐情况选择是否需要进行CheckPoint操作
					if (barrierHandler.processBarrier(checkpointBarrier, offsetChannelIndex(bufferOrEvent.getChannelIndex()), bufferStorage.getPendingBytes())) {
						bufferStorage.rollOver();
					}
				}
			}
			else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
				// 如果是CancelCheckpointMarker类型事件，则调用processCancellationBarrier()方法进行处理
				if (barrierHandler.processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent())) {
					bufferStorage.rollOver();
				}
			}
			else {
				if (bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class) {
					if (barrierHandler.processEndOfPartition()) {
						bufferStorage.rollOver();
					}
				}
				return next;
			}
		}
	}

	private int offsetChannelIndex(int channelIndex) {
		return channelIndex + channelIndexOffset;
	}

	private Optional<BufferOrEvent> handleEmptyBuffer() throws Exception {
		if (!inputGate.isFinished()) {
			return Optional.empty();
		}

		if (endOfInputGate) {
			isFinished = true;
			return Optional.empty();
		} else {
			// end of input stream. stream continues with the buffered data
			endOfInputGate = true;
			barrierHandler.releaseBlocksAndResetBarriers();
			bufferStorage.rollOver();
			return pollNext();
		}
	}

	/**
	 * Checks if the barrier handler has buffered any data internally.
	 * @return {@code True}, if no data is buffered internally, {@code false} otherwise.
	 */
	public boolean isEmpty() {
		return bufferStorage.isEmpty();
	}

	@Override
	public boolean isFinished() {
		return isFinished;
	}

	/**
	 * Cleans up all internally held resources.
	 *
	 * @throws IOException Thrown if the cleanup of I/O resources failed.
	 */
	public void cleanup() throws IOException {
		bufferStorage.close();
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID defining the current pending, or just completed, checkpoint.
	 *
	 * @return The ID of the pending of completed checkpoint.
	 */
	public long getLatestCheckpointId() {
		return barrierHandler.getLatestCheckpointId();
	}

	/**
	 * Gets the time that the latest alignment took, in nanoseconds.
	 * If there is currently an alignment in progress, it will return the time spent in the
	 * current alignment so far.
	 *
	 * @return The duration in nanoseconds
	 */
	public long getAlignmentDurationNanos() {
		return barrierHandler.getAlignmentDurationNanos();
	}

	/**
	 * @return number of underlying input channels.
	 */
	public int getNumberOfInputChannels() {
		return inputGate.getNumberOfInputChannels();
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return barrierHandler.toString();
	}
}
