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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.runtime.shuffle.ShuffleUtils.applyWithShuffleTypeCheck;

/**
 * Factory for {@link SingleInputGate} to use in {@link NettyShuffleEnvironment}.
 */
public class SingleInputGateFactory {
	private static final Logger LOG = LoggerFactory.getLogger(SingleInputGateFactory.class);

	@Nonnull
	private final ResourceID taskExecutorResourceId;

	private final int partitionRequestInitialBackoff;

	private final int partitionRequestMaxBackoff;

	@Nonnull
	private final ConnectionManager connectionManager;

	@Nonnull
	private final ResultPartitionManager partitionManager;

	@Nonnull
	private final TaskEventPublisher taskEventPublisher;

	@Nonnull
	private final NetworkBufferPool networkBufferPool;

	private final int networkBuffersPerChannel;

	private final int floatingNetworkBuffersPerGate;

	private final boolean blockingShuffleCompressionEnabled;

	private final String compressionCodec;

	private final int networkBufferSize;

	public SingleInputGateFactory(
			@Nonnull ResourceID taskExecutorResourceId,
			@Nonnull NettyShuffleEnvironmentConfiguration networkConfig,
			@Nonnull ConnectionManager connectionManager,
			@Nonnull ResultPartitionManager partitionManager,
			@Nonnull TaskEventPublisher taskEventPublisher,
			@Nonnull NetworkBufferPool networkBufferPool) {
		this.taskExecutorResourceId = taskExecutorResourceId;
		this.partitionRequestInitialBackoff = networkConfig.partitionRequestInitialBackoff();
		this.partitionRequestMaxBackoff = networkConfig.partitionRequestMaxBackoff();
		this.networkBuffersPerChannel = networkConfig.networkBuffersPerChannel();
		this.floatingNetworkBuffersPerGate = networkConfig.floatingNetworkBuffersPerGate();
		this.blockingShuffleCompressionEnabled = networkConfig.isBlockingShuffleCompressionEnabled();
		this.compressionCodec = networkConfig.getCompressionCodec();
		this.networkBufferSize = networkConfig.networkBufferSize();
		this.connectionManager = connectionManager;
		this.partitionManager = partitionManager;
		this.taskEventPublisher = taskEventPublisher;
		this.networkBufferPool = networkBufferPool;
	}

	/**
	 * Creates an input gate and all of its input channels.
	 * 1. 和创建ResultPartition的过程一样，在调用createBufferPoolFactory方法中会事先创建bufferPoolFactory（networkBufferPool），用于创建LocalBufferPool。
	 *    通过LocalBufferPool可以为InputGate提供Buffer数据的存储空间，实现本地缓冲InputGate中的二进制数据。
	 * 2. 如果igdd.getConsumedPartitionType().isBlocking()和blockingShuffleCompressionEnabled都为True，则创建bufferDecompressor，这里其实和ResultPartition中的
	 *    BufferCompressor是对应的，即通过BufferDecompressor解压BufferCompressor压缩后的Buffer数据。
	 * 3. 通过InputGateDeploymentDescriptor中的参数BufferCompressor和BufferPoolFactory创建SingleInputGate对象。
	 * 4. 调用createInputChannels()方法创建SingleInputGate中的InputChannels。
	 * 5. 将创建完成的inputGate返回给Task实例。
	 */
	public SingleInputGate create(
			@Nonnull String owningTaskName,
			@Nonnull InputGateDeploymentDescriptor igdd,
			@Nonnull PartitionProducerStateProvider partitionProducerStateProvider,
			@Nonnull InputChannelMetrics metrics) {
		SupplierWithException<BufferPool, IOException> bufferPoolFactory = createBufferPoolFactory(
			networkBufferPool,
			networkBuffersPerChannel,
			floatingNetworkBuffersPerGate,
			igdd.getShuffleDescriptors().length,
			igdd.getConsumedPartitionType());

		BufferDecompressor bufferDecompressor = null;
		if (igdd.getConsumedPartitionType().isBlocking() && blockingShuffleCompressionEnabled) {
			bufferDecompressor = new BufferDecompressor(networkBufferSize, compressionCodec);
		}

		SingleInputGate inputGate = new SingleInputGate(
			owningTaskName,
			igdd.getConsumedResultId(),
			igdd.getConsumedPartitionType(),
			igdd.getConsumedSubpartitionIndex(),
			igdd.getShuffleDescriptors().length,
			partitionProducerStateProvider,
			bufferPoolFactory,
			bufferDecompressor);

		createInputChannels(owningTaskName, igdd, inputGate, metrics);
		return inputGate;
	}

	/**
	 * 1. 从inputGateDeploymentDescriptor中获取ShuffleDescriptor列表，ShuffleDescriptor是在ShuffleMaster中创建和生成的，描述了数据生产者和ResultPartition等信息。
	 * 2. 创建InputChannel[]数组，然后变量InputChannel[]数组，调用createInputChannel()方法创建InputChannel，最后将其存储到inputGate中。可以看出每个resultPartitionID
	 *   对应一个InputChannel。
	 */
	private void createInputChannels(
			String owningTaskName,
			InputGateDeploymentDescriptor inputGateDeploymentDescriptor,
			SingleInputGate inputGate,
			InputChannelMetrics metrics) {
		ShuffleDescriptor[] shuffleDescriptors = inputGateDeploymentDescriptor.getShuffleDescriptors();

		// Create the input channels. There is one input channel for each consumed partition.
		InputChannel[] inputChannels = new InputChannel[shuffleDescriptors.length];

		ChannelStatistics channelStatistics = new ChannelStatistics();

		for (int i = 0; i < inputChannels.length; i++) {
			inputChannels[i] = createInputChannel(
				inputGate,
				i,
				shuffleDescriptors[i],
				channelStatistics,
				metrics);
			ResultPartitionID resultPartitionID = inputChannels[i].getPartitionId();
			inputGate.setInputChannel(resultPartitionID.getPartitionId(), inputChannels[i]);
		}

		LOG.debug("{}: Created {} input channels ({}).",
			owningTaskName,
			inputChannels.length,
			channelStatistics);
	}

	private InputChannel createInputChannel(
			SingleInputGate inputGate,
			int index,
			ShuffleDescriptor shuffleDescriptor,
			ChannelStatistics channelStatistics,
			InputChannelMetrics metrics) {
		return applyWithShuffleTypeCheck(
			NettyShuffleDescriptor.class,
			shuffleDescriptor,
			unknownShuffleDescriptor -> {
				channelStatistics.numUnknownChannels++;
				return new UnknownInputChannel(
					inputGate,
					index,
					unknownShuffleDescriptor.getResultPartitionID(),
					partitionManager,
					taskEventPublisher,
					connectionManager,
					partitionRequestInitialBackoff,
					partitionRequestMaxBackoff,
					metrics,
					networkBufferPool);
			},
			nettyShuffleDescriptor ->
				createKnownInputChannel(
					inputGate,
					index,
					nettyShuffleDescriptor,
					channelStatistics,
					metrics));
	}

	/**
	 * 1. 调用inputChannelDescriptor.getResultPartitionID()方法获取ResultPartitionID
	 * 2. 调用inputChannelDescriptor.isLocalTo(taskExecutorResourceId)方法判断消费数据的Task实例和数据生产的Task实例是否运行在同一个TaskManager中。
	 *    这一步主要是在判断producerLocation和consumerLocation是否相等，如果相等则说明上下游Task属于同一个TaskManager，创建的InputChannel就为LocalInputChannel，
	 *    下游InputChannel不经过网络获取数据。
	 * 3. 如果inputChannelDescriptor.isLocalTo(taskExecutorResourceId)返回false，则说明上下游Task不在同一个TaskManager中，此时创建基于Netty框架实现的
	 *    RemoteInputChannel，帮助下游Task实例从网络中消费上游Task中的Buffer数据。
	 *
	 */
	private InputChannel createKnownInputChannel(
			SingleInputGate inputGate,
			int index,
			NettyShuffleDescriptor inputChannelDescriptor,
			ChannelStatistics channelStatistics,
			InputChannelMetrics metrics) {
		ResultPartitionID partitionId = inputChannelDescriptor.getResultPartitionID();
		if (inputChannelDescriptor.isLocalTo(taskExecutorResourceId)) {
			// Consuming task is deployed to the same TaskManager as the partition => local
			// Task实例属于同一个TaskManager
			channelStatistics.numLocalChannels++;
			return new LocalInputChannel(
				inputGate,
				index,
				partitionId,
				partitionManager,
				taskEventPublisher,
				partitionRequestInitialBackoff,
				partitionRequestMaxBackoff,
				metrics);
		} else {
			// Different instances => remote
			// Task实例不属于同一个TaskManager
			channelStatistics.numRemoteChannels++;
			return new RemoteInputChannel(
				inputGate,
				index,
				partitionId,
				inputChannelDescriptor.getConnectionId(),
				connectionManager,
				partitionRequestInitialBackoff,
				partitionRequestMaxBackoff,
				metrics,
				networkBufferPool);
		}
	}

	@VisibleForTesting
	static SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
			BufferPoolFactory bufferPoolFactory,
			int networkBuffersPerChannel,
			int floatingNetworkBuffersPerGate,
			int size,
			ResultPartitionType type) {
		return () -> bufferPoolFactory.createBufferPool(0, floatingNetworkBuffersPerGate);
	}

	private static class ChannelStatistics {
		int numLocalChannels;
		int numRemoteChannels;
		int numUnknownChannels;

		@Override
		public String toString() {
			return String.format(
				"local: %s, remote: %s, unknown: %s",
				numLocalChannels,
				numRemoteChannels,
				numUnknownChannels);
		}
	}
}
