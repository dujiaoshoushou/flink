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

package org.apache.flink.kubernetes;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.Endpoint;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Kubernetes specific {@link ClusterDescriptor} implementation.
 */
public class KubernetesClusterDescriptor implements ClusterDescriptor<String> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesClusterDescriptor.class);

	private static final String CLUSTER_DESCRIPTION = "Kubernetes cluster";

	private final Configuration flinkConfig;

	private final FlinkKubeClient client;

	private final String clusterId;

	public KubernetesClusterDescriptor(Configuration flinkConfig, FlinkKubeClient client) {
		this.flinkConfig = flinkConfig;
		this.client = client;
		this.clusterId = checkNotNull(
			flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID),
			"ClusterId must be specified!");
	}

	@Override
	public String getClusterDescription() {
		return CLUSTER_DESCRIPTION;
	}

	private ClusterClientProvider<String> createClusterClientProvider(String clusterId) {
		return () -> {
			final Configuration configuration = new Configuration(flinkConfig);

			final Endpoint restEndpoint = client.getRestEndpoint(clusterId);

			if (restEndpoint != null) {
				configuration.setString(RestOptions.ADDRESS, restEndpoint.getAddress());
				configuration.setInteger(RestOptions.PORT, restEndpoint.getPort());
			} else {
				throw new RuntimeException(
						new ClusterRetrieveException(
								"Could not get the rest endpoint of " + clusterId));
			}

			try {
				// Flink client will always use Kubernetes service to contact with jobmanager. So we have a pre-configured web
				// monitor address. Using StandaloneClientHAServices to create RestClusterClient is reasonable.
				return new RestClusterClient<>(
					configuration,
					clusterId,
					new StandaloneClientHAServices(HighAvailabilityServicesUtils.getWebMonitorAddress(
						configuration, HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION)));
			} catch (Exception e) {
				client.handleException(e);
				throw new RuntimeException(new ClusterRetrieveException("Could not create the RestClusterClient.", e));
			}
		};
	}

	@Override
	public ClusterClientProvider<String> retrieve(String clusterId) {
		final ClusterClientProvider<String> clusterClientProvider = createClusterClientProvider(clusterId);

		try (ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient()) {
			LOG.info(
				"Retrieve flink cluster {} successfully, JobManager Web Interface: {}",
				clusterId,
				clusterClient.getWebInterfaceURL());
		}
		return clusterClientProvider;
	}

	@Override
	public ClusterClientProvider<String> deploySessionCluster(ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
		final ClusterClientProvider<String> clusterClientProvider = deployClusterInternal(
			KubernetesSessionClusterEntrypoint.class.getName(),
			clusterSpecification,
			false);

		try (ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient()) {
			LOG.info(
				"Create flink session cluster {} successfully, JobManager Web Interface: {}",
				clusterId,
				clusterClient.getWebInterfaceURL());
		}
		return clusterClientProvider;
	}

	@Override
	public ClusterClientProvider<String> deployJobCluster(
			ClusterSpecification clusterSpecification,
			JobGraph jobGraph,
			boolean detached) throws ClusterDeploymentException {
		throw new ClusterDeploymentException("Per job could not be supported now.");
	}

	/**
	 * 1. 设定ClusterEntrypoint的ExecutionMode为DETACHED或NORMAL类型。
	 * 2. 设定集群启动需要的入口类名称，这里的Entropoint实际上就是KubernetesSessionClusterEntrypoint。
	 * 3. 检查RPC服务的默认端口是否需要更新，如果用户配置了新的端口则需要对原有配置进行更新。
	 * 4. 检查集群高可用配置，如果集群开启高可用模式，则检查高可用的端段等配置信息。
	 * 5. 基于FlinkCubeClient创建KubernetesService服务，使用KubernetesService获取ServiceI，并设定到flinkConfig配置中。
	 *    ServiceID主要用于回收后面的集群资源。
	 * 6. 调用client.createRestService()方法创建Session集群对外提供服务的Rest Service。
	 * 7. 调用client.createConfigMap()方法创建ConfigMap资源对象，存储flink-conf.yaml等配置文件。
	 * 8. 调用client.createFlinkMasterDeployment()方法，创建和启动Flink集群管理节点。
	 * 9. 通过ClusterID创建ClusterClientProvider实现类，通过ClusterClientProvier获取与集群通信的ClusterClient
	 */
	private ClusterClientProvider<String> deployClusterInternal(
			String entryPoint,
			ClusterSpecification clusterSpecification,
			boolean detached) throws ClusterDeploymentException {
		// 设定集群的启动模式为DETACHED或者NORMAL
		final ClusterEntrypoint.ExecutionMode executionMode = detached ?
			ClusterEntrypoint.ExecutionMode.DETACHED
			: ClusterEntrypoint.ExecutionMode.NORMAL;
		flinkConfig.setString(ClusterEntrypoint.EXECUTION_MODE, executionMode.toString());
		// 设定集群启动EntryPoint类名
		flinkConfig.setString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, entryPoint);

		// Rpc, blob, rest, taskManagerRpc ports need to be exposed, so update them to fixed values.
		// 检查和更新RPC服务端口
		KubernetesUtils.checkAndUpdatePortConfigOption(flinkConfig, BlobServerOptions.PORT, Constants.BLOB_SERVER_PORT);
		KubernetesUtils.checkAndUpdatePortConfigOption(
			flinkConfig,
			TaskManagerOptions.RPC_PORT,
			Constants.TASK_MANAGER_RPC_PORT);

		// Set jobmanager address to namespaced service name
		final String nameSpace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
		flinkConfig.setString(JobManagerOptions.ADDRESS, clusterId + "." + nameSpace);
		// 集群高可用配置
		if (HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfig)) {
			flinkConfig.setString(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);
			KubernetesUtils.checkAndUpdatePortConfigOption(
				flinkConfig,
				HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE,
				flinkConfig.get(JobManagerOptions.PORT));
		}

		try {
			// 设定ServiceI，用于集群资源回收GC操作
			final KubernetesService internalSvc = client.createInternalService(clusterId).get();
			// Update the service id in Flink config, it will be used for gc.
			final String serviceId = internalSvc.getInternalResource().getMetadata().getUid();
			if (serviceId != null) {
				flinkConfig.setString(KubernetesConfigOptionsInternal.SERVICE_ID, serviceId);
			} else {
				throw new ClusterDeploymentException("Get service id failed.");
			}

			// Create the rest service when exposed type is not ClusterIp.
			// 创建对外提供服务的Rest Service
			final String restSvcExposedType = flinkConfig.getString(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE);
			if (!restSvcExposedType.equals(KubernetesConfigOptions.ServiceExposedType.ClusterIP.toString())) {
				client.createRestService(clusterId).get();
			}
			// 创建ConfigMap
			client.createConfigMap();
			// 创建和启动Flink集群
			client.createFlinkMasterDeployment(clusterSpecification);
			// 返回 ClusterClient，用于和集群通信
			return createClusterClientProvider(clusterId);
		} catch (Exception e) {
			client.handleException(e);
			throw new ClusterDeploymentException("Could not create Kubernetes cluster " + clusterId, e);
		}
	}

	@Override
	public void killCluster(String clusterId) throws FlinkException {
		try {
			client.stopAndCleanupCluster(clusterId);
		} catch (Exception e) {
			client.handleException(e);
			throw new FlinkException("Could not kill Kubernetes cluster " + clusterId);
		}
	}

	@Override
	public void close() {
		try {
			client.close();
		} catch (Exception e) {
			client.handleException(e);
			LOG.error("failed to close client, exception {}", e.toString());
		}
	}
}
