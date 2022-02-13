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

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CloseRequest;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;

/**
 * Channel handler to initiate data transfers and dispatch backwards flowing task events.
 */
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

	private final ResultPartitionProvider partitionProvider;

	private final TaskEventPublisher taskEventPublisher;

	private final PartitionRequestQueue outboundQueue;

	PartitionRequestServerHandler(
		ResultPartitionProvider partitionProvider,
		TaskEventPublisher taskEventPublisher,
		PartitionRequestQueue outboundQueue) {

		this.partitionProvider = partitionProvider;
		this.taskEventPublisher = taskEventPublisher;
		this.outboundQueue = outboundQueue;
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		super.channelUnregistered(ctx);
	}

	/**
	 * ---------PartitionRequest 处理 --------
	 * 1. 将NettyMessage转换为PartitionRequest类型。
	 * 2. 创建CreditBasedSequenceNumberingViewReader组件，实现对ResultSubPartition中Buffer数据的读取操作，同时基于Credit的反压机制控制
	 *    ResultSubPartition中数据消费的速率。
	 * 3. 调用NetworkSequenceViewReader.requestSubpartitionView()方法，为ResultSubPartition创建Buffer数据消费视图，可以通过ResultSubPartitionView
	 *    消费ResultSubpartition中的Buffer队列的数据。
	 * 4. 调用outboundQueue.notifyReaderCreated(reader)方法，向outboundQueue通知创建reader对象。这里的outboundQueue实际上就是PartitionRequestQueue。
	 *    在PartitionRequestQueue中维护了NetworkSequenceViewReader队列，通过有效的NetworkSequenceViewReader可以消费buffer数据。
	 * --------TaskEventRequest 处理 ---------
	 * 1. TaskEventRequest消息直接调用taskEventPublisher.publish()方法将TaskEvent发送到节点中，用于迭代型的计算场景。
	 * 2. 如果接收到的是CancelPartitionRequest消息，则调用outboundQueue.cancel(request.receiverId)方法进行处理。实际上就是从NetworkSequenceViewReader
	 *    集合中删除创建的NetworkSequenceViewReader对象。
	 * 3. 如果接收到的是CloseRequest消息，则调用outboundQueue.close()方法，释放创建的所有reader。
	 * 4. 如果接收到的是AddCredit消息，则调用outboundQueue.addCredit()方法，增加NetworkSequenceViewReader的信用值，
	 *    从而激活NetworkSequenceViewReader消费Buffer数据。
	 *
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
		try {
			Class<?> msgClazz = msg.getClass();

			// ----------------------------------------------------------------
			// Intermediate result partition requests
			// ----------------------------------------------------------------
			// 如果接收到的是PartitionRequest消息
			if (msgClazz == PartitionRequest.class) {
				PartitionRequest request = (PartitionRequest) msg;

				LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

				try {
					// 创建NetworkSequenceViewReader
					NetworkSequenceViewReader reader;
					reader = new CreditBasedSequenceNumberingViewReader(
						request.receiverId,
						request.credit,
						outboundQueue);
					// 注册SubpartitionView
					reader.requestSubpartitionView(
						partitionProvider,
						request.partitionId,
						request.queueIndex);
					// 向outboundQueue通知Reader被创建
					outboundQueue.notifyReaderCreated(reader);
				} catch (PartitionNotFoundException notFound) {
					respondWithError(ctx, notFound, request.receiverId);
				}
			}
			// ----------------------------------------------------------------
			// Task events
			// ----------------------------------------------------------------
			// TaskEventRequest处理
			else if (msgClazz == TaskEventRequest.class) {
				TaskEventRequest request = (TaskEventRequest) msg;

				if (!taskEventPublisher.publish(request.partitionId, request.event)) {
					respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."), request.receiverId);
				}
			} else if (msgClazz == CancelPartitionRequest.class) { // CancelPartitionRequest处理
				CancelPartitionRequest request = (CancelPartitionRequest) msg;

				outboundQueue.cancel(request.receiverId);
			} else if (msgClazz == CloseRequest.class) { // CloseRequest处理
				outboundQueue.close();
			} else if (msgClazz == AddCredit.class) { // AddCredit处理
				AddCredit request = (AddCredit) msg;

				outboundQueue.addCredit(request.receiverId, request.credit);
			} else {
				LOG.warn("Received unexpected client request: {}", msg);
			}
		} catch (Throwable t) {
			respondWithError(ctx, t);
		}
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error, InputChannelID sourceId) {
		LOG.debug("Responding with error: {}.", error.getClass());

		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
	}
}
