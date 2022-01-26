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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An abstract {@link PipelineExecutor} used to execute {@link Pipeline pipelines} on an existing (session) cluster.
 *
 * @param <ClusterID> the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a client to the target cluster.
 */
@Internal
public class AbstractSessionClusterExecutor<ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>> implements PipelineExecutor {

	private final ClientFactory clusterClientFactory;

	public AbstractSessionClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
		this.clusterClientFactory = checkNotNull(clusterClientFactory);
	}

	/**
	 * 1. 调用ExecutorUtils.getJobGraph方法获取JobGraph。
	 * 2. 通过clusterClientFactory获取ClusterDescriptor，ClusterDescriptor是对不同类型的集群描述，主要用于创建Session集群和获取集群通信的ClusterClient
	 * 3. 调用clusterDescriptor.retrieve(clusterID)，根据指定的ClusterID获取ClusterClientProvider实例。
	 * 4. 通过clusterClientProvider.getClusterClient()方法获取与集群运行时进行网络通信的ClusterClient实例。
	 * 5. 使用clusterClient.submitJob(jobGraph)方法将JobGraph提交到指定的集群的运行时中，然后返回CompletableFuture<JobID>对象。
	 */
	@Override
	public CompletableFuture<JobClient> execute(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration) throws Exception {
		// 调用ExecutorUtils.getJobGraph方法获取JobGraph
		final JobGraph jobGraph = ExecutorUtils.getJobGraph(pipeline, configuration);
		// 通过clusterClientFactory获取ClusterDescriptor
		try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)) {
			final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
			checkState(clusterID != null);
			// 通过clusterID获取clusterClientProvider
			final ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor.retrieve(clusterID);
			// 通过clusterClientProvider获取clusterClient
			ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();
			return clusterClient
					.submitJob(jobGraph) // 提交JobGraph并返回
					.thenApplyAsync(jobID -> (JobClient) new ClusterClientJobClientAdapter<>(
							clusterClientProvider,
							jobID))
					.whenComplete((ignored1, ignored2) -> clusterClient.close());
		}
	}
}
