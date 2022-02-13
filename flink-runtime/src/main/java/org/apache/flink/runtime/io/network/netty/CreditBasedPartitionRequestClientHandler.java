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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.netty.exception.TransportException;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Channel handler to read the messages of buffer response or error response from the
 * producer, to write and flush the unannounced credits for the producer.
 *
 * <p>It is used in the new network credit-based mode.
 */
class CreditBasedPartitionRequestClientHandler extends ChannelInboundHandlerAdapter implements NetworkClientHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CreditBasedPartitionRequestClientHandler.class);

	/** Channels, which already requested partitions from the producers. */
	private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels = new ConcurrentHashMap<>();

	/** Channels, which will notify the producers about unannounced credit. */
	private final ArrayDeque<RemoteInputChannel> inputChannelsWithCredit = new ArrayDeque<>();

	private final AtomicReference<Throwable> channelError = new AtomicReference<>();

	private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

	/**
	 * Set of cancelled partition requests. A request is cancelled iff an input channel is cleared
	 * while data is still coming in for this channel.
	 */
	private final ConcurrentMap<InputChannelID, InputChannelID> cancelled = new ConcurrentHashMap<>();

	/**
	 * The channel handler context is initialized in channel active event by netty thread, the context may also
	 * be accessed by task thread or canceler thread to cancel partition request during releasing resources.
	 */
	private volatile ChannelHandlerContext ctx;

	// ------------------------------------------------------------------------
	// Input channel/receiver registration
	// ------------------------------------------------------------------------

	@Override
	public void addInputChannel(RemoteInputChannel listener) throws IOException {
		checkError();

		inputChannels.putIfAbsent(listener.getInputChannelId(), listener);
	}

	@Override
	public void removeInputChannel(RemoteInputChannel listener) {
		inputChannels.remove(listener.getInputChannelId());
	}

	@Override
	public void cancelRequestFor(InputChannelID inputChannelId) {
		if (inputChannelId == null || ctx == null) {
			return;
		}

		if (cancelled.putIfAbsent(inputChannelId, inputChannelId) == null) {
			ctx.writeAndFlush(new NettyMessage.CancelPartitionRequest(inputChannelId));
		}
	}

	@Override
	public void notifyCreditAvailable(final RemoteInputChannel inputChannel) {
		ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(inputChannel));
	}

	// ------------------------------------------------------------------------
	// Network events
	// ------------------------------------------------------------------------

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		if (this.ctx == null) {
			this.ctx = ctx;
		}

		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// Unexpected close. In normal operation, the client closes the connection after all input
		// channels have been removed. This indicates a problem with the remote task manager.
		if (!inputChannels.isEmpty()) {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
				"Connection unexpectedly closed by remote task manager '" + remoteAddr + "'. "
					+ "This might indicate that the remote task manager was lost.", remoteAddr));
		}

		super.channelInactive(ctx);
	}

	/**
	 * Called on exceptions in the client handler pipeline.
	 *
	 * <p>Remote exceptions are received as regular payload.
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (cause instanceof TransportException) {
			notifyAllChannelsOfErrorAndClose(cause);
		} else {
			final SocketAddress remoteAddr = ctx.channel().remoteAddress();

			final TransportException tex;

			// Improve on the connection reset by peer error message
			if (cause instanceof IOException && cause.getMessage().equals("Connection reset by peer")) {
				tex = new RemoteTransportException("Lost connection to task manager '" + remoteAddr + "'. " +
					"This indicates that the remote task manager was lost.", remoteAddr, cause);
			} else {
				final SocketAddress localAddr = ctx.channel().localAddress();
				tex = new LocalTransportException(
					String.format("%s (connection to '%s')", cause.getMessage(), remoteAddr), localAddr, cause);
			}

			notifyAllChannelsOfErrorAndClose(tex);
		}
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		try {
			decodeMsg(msg);
		} catch (Throwable t) {
			notifyAllChannelsOfErrorAndClose(t);
		}
	}

	/**
	 * Triggered by notifying credit available in the client handler pipeline.
	 *
	 * <p>Enqueues the input channel and will trigger write&flush unannounced credits
	 * for this input channel if it is the first one in the queue.
	 *
	 * 1. 判断msg是否为RemoteInputChannel类型，如果不是则交给ChannelHandlerContext继续处理。
	 * 2. 如果msg是RemoteInputChannel类型，则先获取inputChannelsWithCredit集合，然后判断其是否为空，以此选择是否触发写操作，
	 *    最后添加RemoteInputChannel到inputChannelsWithCredit集合中。inputChannelsWithCredit集合存储了InputChannel及
	 *    其具有的信用值。
	 * 3. 如果inputChannelsWithCredit初始值为空，则调用writeAndFlushNextMessageIfPossible()方法触发发送信用值的操作。
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof RemoteInputChannel) {
			boolean triggerWrite = inputChannelsWithCredit.isEmpty();

			inputChannelsWithCredit.add((RemoteInputChannel) msg);

			if (triggerWrite) {
				writeAndFlushNextMessageIfPossible(ctx.channel());
			}
		} else {
			ctx.fireUserEventTriggered(msg);
		}
	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		writeAndFlushNextMessageIfPossible(ctx.channel());
	}

	private void notifyAllChannelsOfErrorAndClose(Throwable cause) {
		if (channelError.compareAndSet(null, cause)) {
			try {
				for (RemoteInputChannel inputChannel : inputChannels.values()) {
					inputChannel.onError(cause);
				}
			} catch (Throwable t) {
				// We can only swallow the Exception at this point. :(
				LOG.warn("An Exception was thrown during error notification of a remote input channel.", t);
			} finally {
				inputChannels.clear();
				inputChannelsWithCredit.clear();

				if (ctx != null) {
					ctx.close();
				}
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Checks for an error and rethrows it if one was reported.
	 */
	private void checkError() throws IOException {
		final Throwable t = channelError.get();

		if (t != null) {
			if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new IOException("There has been an error in the channel.", t);
			}
		}
	}

	/**
	 * 1. 如果接入的数据为NettyMessage.BufferResponse类型，则将msg转换成NettyMessage.BufferResponse数据，并通过数据中的receiverId获取
	 *    RemoteInputChannel，这里的receiverId实际上就是InputChannelId。
	 * 2. 如果inputChannel为空，则释放bufferOrEvent，然后调用cancelRequestFor()方法向上游发送CancelPartitionRequest。
	 * 3. 如果InputChannel不为空，则调用decodeBufferOrEvent()方法对BufferOrEvetn进行解析和处理。
	 * 4. 如果上游传输的数据为NettyMessage.ErrorResponse消息，则将数据转换为NettyMessage.ErrorResponse类型，然后判断错误释放为FatalError，
	 *    如果是则调用notifyAllChannelsOfErrorAndClose()方法关闭所有的InputChannel。
	 * 5. 如果不是FatalError，且错误为PartitionNotFoundExecption,则调用InputChannel.onFailedPartitionRequest()方法进行处理。
	 *    其他情况则抛出异常，然后调用InputChannel.onError()方法将错误传递给InputChannel进行处理。
	 */
	private void decodeMsg(Object msg) throws Throwable {
		final Class<?> msgClazz = msg.getClass();

		// ---- Buffer --------------------------------------------------------
		if (msgClazz == NettyMessage.BufferResponse.class) {
			NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;

			RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);
			if (inputChannel == null) {
				bufferOrEvent.releaseBuffer();

				cancelRequestFor(bufferOrEvent.receiverId);

				return;
			}
			// 解析BufferOrEvent数据
			decodeBufferOrEvent(inputChannel, bufferOrEvent);

		} else if (msgClazz == NettyMessage.ErrorResponse.class) {
			// ---- Error ---------------------------------------------------------
			NettyMessage.ErrorResponse error = (NettyMessage.ErrorResponse) msg;

			SocketAddress remoteAddr = ctx.channel().remoteAddress();

			if (error.isFatalError()) {
				notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
					"Fatal error at remote task manager '" + remoteAddr + "'.",
					remoteAddr,
					error.cause));
			} else {
				RemoteInputChannel inputChannel = inputChannels.get(error.receiverId);

				if (inputChannel != null) {
					if (error.cause.getClass() == PartitionNotFoundException.class) {
						inputChannel.onFailedPartitionRequest();
					} else {
						inputChannel.onError(new RemoteTransportException(
							"Error at remote task manager '" + remoteAddr + "'.",
							remoteAddr,
							error.cause));
					}
				}
			}
		} else {
			throw new IllegalStateException("Received unknown message from producer: " + msg.getClass());
		}
	}

	/**
	 * ------ Buffer 类型数据 -------
	 * 1. 通过receivedSize判断接入的Buffer数据是否为空，如果为空则调用InputChannel.onEmptyBuffer()方法进行处理。
	 * 2. 如果接入的Buffer数据不为空，则通过InputChannel申请Buffer内存空间。
	 * 3. 调用nettyBuffer.readBytes(buffer.asByteBuf(),receivedSize)方法，将接入的ByteBufnettyBuffer数据写入申请的Buffer内存空间中。
	 * 4. 将bufferOrEvent中是否压缩对应的配置设定到申请的Buffer数据中。
	 * 5. 调用InputChannel.onBuffer()方法继续通过InputChannel处理Buffer数据，同时会携带sequenceNumber和Backlog等指标。
	 * ----- Event 类型数据 ------
	 * 1. 根据receiveSize临时创建byte[]数组空间，可以看出对于Event数据直接使用了堆内存空间。
	 * 2. 将ByteBuf nettyBuffer数据写入byte[] byteArray数组中。
	 * 3. 调用 MemorySegmentFactory.wrap(byteArray)方法，将byte[]转换为MemorySegment
	 * 4. 通过MemorySegment对象调用 new NetworkBuffer()构造器方法创建NetworkBuffer对象。
	 * 5. 调用InputChannel.onBuffer()方法将Buffer交给InputChannel继续处理。
	 */
	private void decodeBufferOrEvent(RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent) throws Throwable {
		try {
			// 从bufferOrEvent中获取nettyBuffer
			ByteBuf nettyBuffer = bufferOrEvent.getNettyBuffer();
			// 数据大小
			final int receivedSize = nettyBuffer.readableBytes();
			if (bufferOrEvent.isBuffer()) {
				// ---- Buffer ------------------------------------------------

				// Early return for empty buffers. Otherwise Netty's readBytes() throws an
				// IndexOutOfBoundsException.
				if (receivedSize == 0) {
					inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
					return;
				}
				// 从InputChannel中申请Buffer
				Buffer buffer = inputChannel.requestBuffer();
				if (buffer != null) {
					// 调用nettyBuffer.readBytes()
					nettyBuffer.readBytes(buffer.asByteBuf(), receivedSize);
					buffer.setCompressed(bufferOrEvent.isCompressed);
					// 调用InputChannel.onBuffer()方法处理接入的数据
					inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
				} else if (inputChannel.isReleased()) {
					cancelRequestFor(bufferOrEvent.receiverId);
				} else {
					throw new IllegalStateException("No buffer available in credit-based input channel.");
				}
			} else {
				// ---- Event -------------------------------------------------
				// TODO We can just keep the serialized data in the Netty buffer and release it later at the reader
				// Event类型
				// 根据receivedSize创建字节数据
				byte[] byteArray = new byte[receivedSize];
				// 将nettyBuffer数据写入到byteArray中
				nettyBuffer.readBytes(byteArray);
				// 创建MemorySegment
				MemorySegment memSeg = MemorySegmentFactory.wrap(byteArray);
				// 创建NetworkBuffer
				Buffer buffer = new NetworkBuffer(memSeg, FreeingBufferRecycler.INSTANCE, false, receivedSize);
				// 调用InputChannel.onBuffer()方法进行处理
				inputChannel.onBuffer(buffer, bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
			}
		} finally {
			bufferOrEvent.releaseBuffer();
		}
	}

	/**
	 * Tries to write&flush unannounced credits for the next input channel in queue.
	 *
	 * <p>This method may be called by the first input channel enqueuing, or the complete
	 * future's callback in previous input channel, or the channel writability changed event.
	 * 1. 判断TCP通道是否处于正常状态，如果TCP通道不可写，则直接返回空。
	 * 2. 循环获取inputChannelsWithCredit中的InputChannel，并判断其是否为空，如果为空则直接返回。
	 * 3. 判断InputChannel是否已经释放，因为没有必要为已经释放的InputChannel向ResultPartition通知信用值。
	 *    如果没有释放则创建AddCreadit请求，其中包含PartitionID、InputChannelId等信息，同时通过getAndResetUnannouncedCredit()方法
	 *    获取InputChannel的unannouncedCredit指标，将unannouncedCredit清零。
	 * 4. 调用netty channel向TCP Channel 中写入AddCredit请求，此时ResultPartition上游会收到AddCredit请求。
	 */
	private void writeAndFlushNextMessageIfPossible(Channel channel) {
		if (channelError.get() != null || !channel.isWritable()) {
			return;
		}

		while (true) {
			RemoteInputChannel inputChannel = inputChannelsWithCredit.poll();

			// The input channel may be null because of the write callbacks
			// that are executed after each write.
			if (inputChannel == null) {
				return;
			}

			//It is no need to notify credit for the released channel.
			if (!inputChannel.isReleased()) {
				AddCredit msg = new AddCredit(
					inputChannel.getPartitionId(),
					inputChannel.getAndResetUnannouncedCredit(),
					inputChannel.getInputChannelId());

				// Write and flush and wait until this is done before
				// trying to continue with the next input channel.
				channel.writeAndFlush(msg).addListener(writeListener);

				return;
			}
		}
	}

	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			try {
				if (future.isSuccess()) {
					writeAndFlushNextMessageIfPossible(future.channel());
				} else if (future.cause() != null) {
					notifyAllChannelsOfErrorAndClose(future.cause());
				} else {
					notifyAllChannelsOfErrorAndClose(new IllegalStateException("Sending cancelled by user."));
				}
			} catch (Throwable t) {
				notifyAllChannelsOfErrorAndClose(t);
			}
		}
	}
}
