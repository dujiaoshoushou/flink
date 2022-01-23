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

package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.ExecutorService;

/**
 * {@link RestEndpointFactory} which creates a {@link DispatcherRestEndpoint}.
 */
public enum SessionRestEndpointFactory implements RestEndpointFactory<DispatcherGateway> {
	INSTANCE;

	/**
	 *
	 * @param configuration 集群配置参数
	 * @param dispatcherGatewayRetriever DispatcherGateway服务地址获取器，用于获取当前活跃的dispatcherGateway地址。基于dispatcherGateway可以实现与Dispatcher的RPC通信。
	 *                                   最终提交的JobGraph通过dispatcherGateway发送给Dispatcher组件。
	 * @param resourceManagerGatewayRetriever ResourceManagerGateway服务地址获取器，用于获取当前活跃的ResourceManagerGateway地址，
	 *                                        通过ResourceManagerGateway实现ResourceManager组件之间的RPC通信，例如在TaskManagerHandler
	 *                                        中通过用于ResourceManagerGateway获取集群中的TaskManager监控信息。
	 * @param transientBlobService 临时二进制对象数据存储服务，BlobServer接收数据后，会及时清理Cache中的对象数据。
	 * @param executor 用于处理WebMonitorEndpoint请求线程池服务。
	 * @param metricFetcher 用于拉取JobManager和TaskManager上的Metric监控指标。
	 * @param leaderElectionService 用于在高可用集群中启动和选择服务的Leader节点，如通过leaderElectionService启动WebMonitorEndpoint
	 *                              RPC服务，然后将Leader节点注册至ZooKeeper，以此实现WebMonitorEndpoint服务的高可用。
	 * @param fatalErrorHandler 异常处理器，当WebMonitorEndpoint出现异常时调用fatalErrorHandler的中处理接口。
	 * @return
	 * @throws Exception
	 */
	@Override
	public WebMonitorEndpoint<DispatcherGateway> createRestEndpoint(
			Configuration configuration,
			LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
			LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
			TransientBlobService transientBlobService,
			ExecutorService executor,
			MetricFetcher metricFetcher,
			LeaderElectionService leaderElectionService,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		// TODO 通过configuration获取RestHandlerConfiguration
		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(configuration);
		// TODO 创建DispatcherRestEndpoint
		return new DispatcherRestEndpoint(
			RestServerEndpointConfiguration.fromConfiguration(configuration),
			dispatcherGatewayRetriever,
			configuration,
			restHandlerConfiguration,
			resourceManagerGatewayRetriever,
			transientBlobService,
			executor,
			metricFetcher,
			leaderElectionService,
			fatalErrorHandler);
	}
}
