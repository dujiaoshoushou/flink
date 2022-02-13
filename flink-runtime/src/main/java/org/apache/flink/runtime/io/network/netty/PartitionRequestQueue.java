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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;

/**
 * A nonEmptyReader of partition queues, which listens for channel writability changed
 * events before writing and flushing {@link Buffer} instances.
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	/** The readers which are already enqueued available for transferring data. */
	private final ArrayDeque<NetworkSequenceViewReader> availableReaders = new ArrayDeque<>();

	/** All the readers created for the consumers' partition requests. */
	private final ConcurrentMap<InputChannelID, NetworkSequenceViewReader> allReaders = new ConcurrentHashMap<>();

	private boolean fatalError;

	private ChannelHandlerContext ctx;

	@Override
	public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelRegistered(ctx);
	}

	void notifyReaderNonEmpty(final NetworkSequenceViewReader reader) {
		// The notification might come from the same thread. For the initial writes this
		// might happen before the reader has set its reference to the view, because
		// creating the queue and the initial notification happen in the same method call.
		// This can be resolved by separating the creation of the view and allowing
		// notifications.

		// TODO This could potentially have a bad performance impact as in the
		// worst case (network consumes faster than the producer) each buffer
		// will trigger a separate event loop task being scheduled.
		ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(reader));
	}

	/**
	 * Try to enqueue the reader once receiving credit notification from the consumer or receiving
	 * non-empty reader notification from the producer.
	 *
	 * <p>NOTE: Only one thread would trigger the actual enqueue after checking the reader's
	 * availability, so there is no race condition here.
	 */
	private void enqueueAvailableReader(final NetworkSequenceViewReader reader) throws Exception {
		if (reader.isRegisteredAsAvailable() || !reader.isAvailable()) {
			return;
		}
		// Queue an available reader for consumption. If the queue is empty,
		// we try trigger the actual write. Otherwise this will be handled by
		// the writeAndFlushNextMessageIfPossible calls.
		boolean triggerWrite = availableReaders.isEmpty();
		registerAvailableReader(reader);

		if (triggerWrite) {
			writeAndFlushNextMessageIfPossible(ctx.channel());
		}
	}

	/**
	 * Accesses internal state to verify reader registration in the unit tests.
	 *
	 * <p><strong>Do not use anywhere else!</strong>
	 *
	 * @return readers which are enqueued available for transferring data
	 */
	@VisibleForTesting
	ArrayDeque<NetworkSequenceViewReader> getAvailableReaders() {
		return availableReaders;
	}

	public void notifyReaderCreated(final NetworkSequenceViewReader reader) {
		allReaders.put(reader.getReceiverId(), reader);
	}

	public void cancel(InputChannelID receiverId) {
		ctx.pipeline().fireUserEventTriggered(receiverId);
	}

	public void close() throws IOException {
		if (ctx != null) {
			ctx.channel().close();
		}

		for (NetworkSequenceViewReader reader : allReaders.values()) {
			releaseViewReader(reader);
		}
		allReaders.clear();
	}

	/**
	 * Adds unannounced credits from the consumer and enqueues the corresponding reader for this
	 * consumer (if not enqueued yet).
	 *
	 * @param receiverId The input channel id to identify the consumer.
	 * @param credit The unannounced credits of the consumer.
	 */
	void addCredit(InputChannelID receiverId, int credit) throws Exception {
		if (fatalError) {
			return;
		}

		NetworkSequenceViewReader reader = allReaders.get(receiverId);
		if (reader != null) {
			reader.addCredit(credit); // 向读取器中添加信用值

			enqueueAvailableReader(reader); // 将读取器加入availableReaders队列
		} else {
			throw new IllegalStateException("No reader for receiverId = " + receiverId + " exists.");
		}
	}

	/**
	 * 1. 如果msg是NetworkSequenceViewReader类型，则调用enqueueAvailableReader()方法激活当前的读取器。
	 * 2. 如果msg是InputChannelID类型，则释放当前的InputChannel信息，从availablereader队列中删除InputChannelID对应的读取器，
	 *    同时从allReader队列中删除对应的读取器。
	 * 3. 如果不是NetworkSequenceViewReader和InputChannelID类型事件，则继续通过ctx.fireUserEventTriggered(msg)方法将
	 *    msg下发到后续的ChannelHandler中处理。
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		// The user event triggered event loop callback is used for thread-safe
		// hand over of reader queues and cancelled producers.
		// 如果msg是NetworkSequenceViewReader类型，则调用enqueueAvailableReader()方法激活当前的读取器
		if (msg instanceof NetworkSequenceViewReader) {
			enqueueAvailableReader((NetworkSequenceViewReader) msg);
		} else if (msg.getClass() == InputChannelID.class) { // 如果msg是InputChannelID类型，则释放InputChannel信息
			// Release partition view that get a cancel request.
			InputChannelID toCancel = (InputChannelID) msg;

			// remove reader from queue of available readers
			// 从availavle reader 队列中删除InputChannelID对应的读取器
			availableReaders.removeIf(reader -> reader.getReceiverId().equals(toCancel));

			// remove reader from queue of all readers and release its resource
			// 从allReader队列中删除Reader并释放资源
			final NetworkSequenceViewReader toRelease = allReaders.remove(toCancel);
			if (toRelease != null) {
				releaseViewReader(toRelease);
			}
		} else {
			// 其他事件则不进行处理，继续下发。
			ctx.fireUserEventTriggered(msg);
		}
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextMessageIfPossible(ctx.channel());
	}

	/**
	 * 1. 循环不断的从availableReader队列中取出可用的读取器读取Buffer数据。
	 * 2. NetworkSequenceViewReader会通过ResultSubPartitionView从ResultSubPartition中读取Buffer队列的数据，接着赋值给next对象。
	 * 3. 此时如果next数据为空，则调用reader.getFailureCause()方法获取具体原因，并将ErrorResponse发送到ChannelPipeline中，继续进行处理。
	 * 4. 如果next.moreAvailable()为true，说明ResultPartition中还有Buffer数据，则将当前NetworkSequenceViewReader添加到AvailavleReader队列中
	 *    继续读取Buffer数据。
	 * 5. 将读取出来的Buffer数据构建成BufferResponse对象，并携带Backlog等信息，最后调用channel.writeAndFlush(msg)方法推送到相应的TCP通道中。
	 */
	private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
		if (fatalError || !channel.isWritable()) {
			return;
		}

		// The logic here is very similar to the combined input gate and local
		// input channel logic. You can think of this class acting as the input
		// gate and the consumed views as the local input channels.

		BufferAndAvailability next = null;
		try {
			while (true) {
				// 获取CreditBasedSequenceNumberingViewReader
				NetworkSequenceViewReader reader = pollAvailableReader();

				// No queue with available data. We allow this here, because
				// of the write callbacks that are executed after each write.
				if (reader == null) {
					return;
				}
				next = reader.getNextBuffer();
				// 如果数据为空，则查看具体原因，并发送到ctx中
				if (next == null) {
					if (!reader.isReleased()) {
						continue;
					}

					Throwable cause = reader.getFailureCause();
					if (cause != null) {
						ErrorResponse msg = new ErrorResponse(
							new ProducerFailedException(cause),
							reader.getReceiverId());

						ctx.writeAndFlush(msg);
					}
				} else {
					// This channel was now removed from the available reader queue.
					// We re-add it into the queue if it is still available
					// 如果next提示还有很多数据，则继续将读取器添加到AvailableReader队列中
					if (next.moreAvailable()) {
						registerAvailableReader(reader);
					}
					// 构建BufferResponse数据，封装Buffer数据
					BufferResponse msg = new BufferResponse(
						next.buffer(),
						reader.getSequenceNumber(),
						reader.getReceiverId(),
						next.buffersInBacklog());

					// Write and flush and wait until this is done before
					// trying to continue with the next buffer.
					// 将数据输出到指定的TCP通道中
					channel.writeAndFlush(msg).addListener(writeListener);

					return;
				}
			}
		} catch (Throwable t) {
			if (next != null) {
				next.buffer().recycleBuffer();
			}

			throw new IOException(t.getMessage(), t);
		}
	}

	private void registerAvailableReader(NetworkSequenceViewReader reader) {
		availableReaders.add(reader);
		reader.setRegisteredAsAvailable(true);
	}

	@Nullable
	private NetworkSequenceViewReader pollAvailableReader() {
		NetworkSequenceViewReader reader = availableReaders.poll();
		if (reader != null) {
			reader.setRegisteredAsAvailable(false);
		}
		return reader;
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		releaseAllResources();

		ctx.fireChannelInactive();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		handleException(ctx.channel(), cause);
	}

	private void handleException(Channel channel, Throwable cause) throws IOException {
		LOG.error("Encountered error while consuming partitions", cause);

		fatalError = true;
		releaseAllResources();

		if (channel.isActive()) {
			channel.writeAndFlush(new ErrorResponse(cause)).addListener(ChannelFutureListener.CLOSE);
		}
	}

	private void releaseAllResources() throws IOException {
		// note: this is only ever executed by one thread: the Netty IO thread!
		for (NetworkSequenceViewReader reader : allReaders.values()) {
			releaseViewReader(reader);
		}

		availableReaders.clear();
		allReaders.clear();
	}

	private void releaseViewReader(NetworkSequenceViewReader reader) throws IOException {
		reader.setRegisteredAsAvailable(false);
		reader.releaseAllResources();
	}

	// This listener is called after an element of the current nonEmptyReader has been
	// flushed. If successful, the listener triggers further processing of the
	// queues.
	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					writeAndFlushNextMessageIfPossible(future.channel());
				} else if (future.cause() != null) {
					handleException(future.channel(), future.cause());
				} else {
					handleException(future.channel(), new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				handleException(future.channel(), t);
			}
		}
	}
}
