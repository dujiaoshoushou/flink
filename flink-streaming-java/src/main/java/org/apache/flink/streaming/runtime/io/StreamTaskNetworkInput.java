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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of {@link StreamTaskInput} that wraps an input from network taken from {@link CheckpointedInputGate}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link StreamStatus} events, and forwards them to event subscribers once the
 * {@link StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or
 * that a {@link StreamStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status status elements must be protected by synchronizing
 * on the given lock object. This ensures that we don't call methods on a
 * {@link StreamInputProcessor} concurrently with the timer callback or other things.
 */
@Internal
public final class StreamTaskNetworkInput<T> implements StreamTaskInput<T> {

	private final CheckpointedInputGate checkpointedInputGate;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	/** Valve that controls how watermarks and stream statuses are forwarded. */
	private final StatusWatermarkValve statusWatermarkValve;

	private final int inputIndex;

	private int lastChannel = UNSPECIFIED;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer = null;

	@SuppressWarnings("unchecked")
	public StreamTaskNetworkInput(
			CheckpointedInputGate checkpointedInputGate,
			TypeSerializer<?> inputSerializer,
			IOManager ioManager,
			StatusWatermarkValve statusWatermarkValve,
			int inputIndex) {
		this.checkpointedInputGate = checkpointedInputGate;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[checkpointedInputGate.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
		this.inputIndex = inputIndex;
	}

	@VisibleForTesting
	StreamTaskNetworkInput(
		CheckpointedInputGate checkpointedInputGate,
		TypeSerializer<?> inputSerializer,
		StatusWatermarkValve statusWatermarkValve,
		int inputIndex,
		RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers) {

		this.checkpointedInputGate = checkpointedInputGate;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));
		this.recordDeserializers = recordDeserializers;
		this.statusWatermarkValve = statusWatermarkValve;
		this.inputIndex = inputIndex;
	}

	/**
	 * 1. 循环调用checkpointedInputGate.pollNext()方法获取数据，从网络接入的数据是通过InputGate的InputChannel接入的。
	 * 2. 从InputGate中接入的数据格式为Optional bufferOrEvent，换句话讲，网络传输的数据即包含buffer类型数据，也含有事件数据，这里的事件指得是Watermark等。
	 * 3. 如果产生了bufferOrEvent数据，则会调用processBufferOrEvent()方法进行处理，主要会对bufferOrEvent数据进行反序列化处理，转成成具体的StreamRecord。
	 * 4.解析出来的StreamRecord数据会存放在currentRecordDeserializer实例中，通过判断currentRecordDeserializer是否为空，
	 *   从currentRecordDeserializer获取DeserializationResult
	 * 5. 如果获取结果中的Buffer已经被消费，则对Buffer数据占用的内存空间进行回收。
	 * 6. 如果获取结果是完整的Record记录，则调用processElement()方法对数据元素进行处理。
	 * 7. 处理完毕后退出循环并返回MORE_AVAILAVLE状态，继续等待新的数据接入。
	 *
	 * 第7章节，详细逻辑如下：
	 * 1. 启动一个While(true)循环并根据指定条件退出循环。
	 * 2. 判断currentRecordDeserializer是否为空，如果不为空，表明currentRecordDeserializer对象中已经含有反序列化的数据元素，此时会优先从中获取反序列化的数据元素，
	 *    并返回DeserializationResult表示数据元素的消费情况。
	 * 3. 如果DeserializationResult中显示Buffer已经消费完，则对Buffer内存空间进行回收，本缓冲区中的数据元素都会通过Buffer结构以二进制的格式进行存储。
	 * 4. 判断DeserializationResult是否消费了完成的Record，如果是则表明当前反序列化的Buffer数据是一个完整的数据元素。接着调用processElement()方法对该数据元素继续进行
	 *    处理，并返回InputStatus.MORE_AVAILABLE状态，表示管道中还有更多的数据元素可以继续处理。
	 * 5. 当数据还没有接入调度时候，currentRecordDeserializer对象为空，此时会跳过上面的逻辑，从InputGate中拉取新的Buffer数据，并调用processBufferOrEvent()方法
	 *    将接收到的Buffer数据写入currentRecordDeserializer。
	 * 6. 调用checkpointedInputGate.pollNext()方法从InputGate中拉取新的BufferOrEvent数据，BufferOrEvent代表数据元素可以是Buffer类型，也可以是事件类型，
	 *    比如CheckpointBarrier、TaskEvent等事件。
	 * 7. bufferOrEvent不为空的时候，会调用processBufferOrEvent()进行处理，此时如果是Buffer类型的数据则进行反序列化操作，将接收到的二进制数据存储到currentRecordDeserializer中，
	 *    再从currentRecordDeserializer对象汇总获取数据元素。对于事件数据则直接执行相应类型事件的操作。
	 * 8. 如果bufferOrEvent为空，则判断checkpointedInputGate是否已经关闭，如果已经关闭了则直接返回END_OF_INPUT状态，否则返回NOTHING_AVAILABLE状态。
	 */
	@Override
	public InputStatus emitNext(DataOutput<T> output) throws Exception {
		// 循环获取数据
		while (true) {
			// get the stream element from the deserializer
			// 从currentRecordDeserializer中获取StreamElement
			if (currentRecordDeserializer != null) {
				// 获取DeserializationResult
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
				// 如果Buffer已经被消费，则对Buffer数据占用的内存空间进行回收
				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}
				// 如果结果是完整的Record记录，则调用processElement()方法进行处理
				if (result.isFullRecord()) {
					processElement(deserializationDelegate.getInstance(), output);
					// 处理完毕后退出循环并返回MORE_AVAILABLE状态，等待下一次计算
					return InputStatus.MORE_AVAILABLE;
				}
			}
			// 从checkpointedInputGate中拉取数据
			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
			// 如果bufferOrEvent有数据产生，则调用processBufferOrEvent()进行处理
			if (bufferOrEvent.isPresent()) {
				processBufferOrEvent(bufferOrEvent.get());
			} else {
				// 如果checkpointedInputGate中已经没有数据，则返回END_OF_INPUT结束计算，否则返回NOTHING_AVAILABLE
				if (checkpointedInputGate.isFinished()) {
					checkState(checkpointedInputGate.getAvailableFuture().isDone(), "Finished BarrierHandler should be available");
					if (!checkpointedInputGate.isEmpty()) {
						throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
					}
					return InputStatus.END_OF_INPUT;
				}
				return InputStatus.NOTHING_AVAILABLE;
			}
		}
	}

	private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
		if (recordOrMark.isRecord()){
			// 处理StreamRecord类型数据
			output.emitRecord(recordOrMark.asRecord());
		} else if (recordOrMark.isWatermark()) {
			// 处理Watermark类型数据
			statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel);
		} else if (recordOrMark.isLatencyMarker()) {
			// 处理LatencyMarker类型数据
			output.emitLatencyMarker(recordOrMark.asLatencyMarker());
		} else if (recordOrMark.isStreamStatus()) {
			// 处理StreamStatus类型数据
			statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel);
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement");
		}
	}

	private void processBufferOrEvent(BufferOrEvent bufferOrEvent) throws IOException {
		if (bufferOrEvent.isBuffer()) {
			lastChannel = bufferOrEvent.getChannelIndex();
			checkState(lastChannel != StreamTaskInput.UNSPECIFIED);
			currentRecordDeserializer = recordDeserializers[lastChannel];
			checkState(currentRecordDeserializer != null,
				"currentRecordDeserializer has already been released");

			currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
		}
		else {
			// Event received
			final AbstractEvent event = bufferOrEvent.getEvent();
			// TODO: with checkpointedInputGate.isFinished() we might not need to support any events on this level.
			if (event.getClass() != EndOfPartitionEvent.class) {
				throw new IOException("Unexpected event: " + event);
			}

			// release the record deserializer immediately,
			// which is very valuable in case of bounded stream
			releaseDeserializer(bufferOrEvent.getChannelIndex());
		}
	}

	@Override
	public int getInputIndex() {
		return inputIndex;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (currentRecordDeserializer != null) {
			return AVAILABLE;
		}
		return checkpointedInputGate.getAvailableFuture();
	}

	@Override
	public void close() throws IOException {
		// release the deserializers . this part should not ever fail
		for (int channelIndex = 0; channelIndex < recordDeserializers.length; channelIndex++) {
			releaseDeserializer(channelIndex);
		}

		// cleanup the resources of the checkpointed input gate
		checkpointedInputGate.cleanup();
	}

	private void releaseDeserializer(int channelIndex) {
		RecordDeserializer<?> deserializer = recordDeserializers[channelIndex];
		if (deserializer != null) {
			// recycle buffers and clear the deserializer.
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();

			recordDeserializers[channelIndex] = null;
		}
	}
}
