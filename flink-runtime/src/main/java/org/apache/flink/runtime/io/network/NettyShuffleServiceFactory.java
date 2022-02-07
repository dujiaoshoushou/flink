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

package org.apache.flink.runtime.io.network;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceFactory;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;

import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerShuffleMetrics;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Netty based shuffle service implementation.
 */
public class NettyShuffleServiceFactory implements ShuffleServiceFactory<NettyShuffleDescriptor, ResultPartition, SingleInputGate> {

	private static final String DIR_NAME_PREFIX = "netty-shuffle";

	@Override
	public NettyShuffleMaster createShuffleMaster(Configuration configuration) {
		return NettyShuffleMaster.INSTANCE;
	}

	@Override
	public NettyShuffleEnvironment createShuffleEnvironment(ShuffleEnvironmentContext shuffleEnvironmentContext) {
		checkNotNull(shuffleEnvironmentContext);
		NettyShuffleEnvironmentConfiguration networkConfig = NettyShuffleEnvironmentConfiguration.fromConfiguration(
			shuffleEnvironmentContext.getConfiguration(),
			shuffleEnvironmentContext.getNetworkMemorySize(),
			shuffleEnvironmentContext.isLocalCommunicationOnly(),
			shuffleEnvironmentContext.getHostAddress());
		return createNettyShuffleEnvironment(
			networkConfig,
			shuffleEnvironmentContext.getTaskExecutorResourceId(),
			shuffleEnvironmentContext.getEventPublisher(),
			shuffleEnvironmentContext.getParentMetricGroup());
	}

	@VisibleForTesting
	static NettyShuffleEnvironment createNettyShuffleEnvironment(
			NettyShuffleEnvironmentConfiguration config,
			ResourceID taskExecutorResourceId,
			TaskEventPublisher taskEventPublisher,
			MetricGroup metricGroup) {
		// 检查参数都不能为空
		checkNotNull(config);
		checkNotNull(taskExecutorResourceId);
		checkNotNull(taskEventPublisher);
		checkNotNull(metricGroup);
		// 获取netty相关的配置参数
		NettyConfig nettyConfig = config.nettyConfig();
		// 创建ResultPartitionManager实例
		ResultPartitionManager resultPartitionManager = new ResultPartitionManager();
		// 创建FileChannelManager实例
		FileChannelManager fileChannelManager = new FileChannelManagerImpl(config.getTempDirs(), DIR_NAME_PREFIX);
		// 创建ConnectionManager实例
		ConnectionManager connectionManager = nettyConfig != null ?
			new NettyConnectionManager(resultPartitionManager, taskEventPublisher, nettyConfig) :
			new LocalConnectionManager();
		// 创建NetworkBufferPool实例
		NetworkBufferPool networkBufferPool = new NetworkBufferPool(
			config.numNetworkBuffers(),
			config.networkBufferSize(),
			config.networkBuffersPerChannel(),
			config.getRequestSegmentsTimeout());
		// 注册ShuffleMetrics信息
		registerShuffleMetrics(metricGroup, networkBufferPool);
		// 创建ResultPartitionFactory实例
		ResultPartitionFactory resultPartitionFactory = new ResultPartitionFactory(
			resultPartitionManager,
			fileChannelManager,
			networkBufferPool,
			config.getBlockingSubpartitionType(),
			config.networkBuffersPerChannel(),
			config.floatingNetworkBuffersPerGate(),
			config.networkBufferSize(),
			config.isForcePartitionReleaseOnConsumption(),
			config.isBlockingShuffleCompressionEnabled(),
			config.getCompressionCodec());
		// 创建SingleInputGateFactory实例
		SingleInputGateFactory singleInputGateFactory = new SingleInputGateFactory(
			taskExecutorResourceId,
			config,
			connectionManager,
			resultPartitionManager,
			taskEventPublisher,
			networkBufferPool);
		// 最后返回NettyShuffleEnvironment
		return new NettyShuffleEnvironment(
			taskExecutorResourceId,
			config,
			networkBufferPool,
			connectionManager,
			resultPartitionManager,
			fileChannelManager,
			resultPartitionFactory,
			singleInputGateFactory);
	}
}
