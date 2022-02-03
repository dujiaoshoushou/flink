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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.RestartTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link ExecutionGraph} from a {@link JobGraph}.
 */
public class ExecutionGraphBuilder {

	/**
	 * Builds the ExecutionGraph from the JobGraph.
	 * If a prior execution graph exists, the JobGraph will be attached. If no prior execution
	 * graph exists, then the JobGraph will become attach to a new empty execution graph.
	 */
	public static ExecutionGraph buildGraph(
			@Nullable ExecutionGraph prior,
			JobGraph jobGraph,
			Configuration jobManagerConfig,
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			SlotProvider slotProvider,
			ClassLoader classLoader,
			CheckpointRecoveryFactory recoveryFactory,
			Time rpcTimeout,
			RestartStrategy restartStrategy,
			MetricGroup metrics,
			BlobWriter blobWriter,
			Time allocationTimeout,
			Logger log,
			ShuffleMaster<?> shuffleMaster,
			JobMasterPartitionTracker partitionTracker) throws JobExecutionException, JobException {

		final FailoverStrategy.Factory failoverStrategy =
			FailoverStrategyLoader.loadFailoverStrategy(jobManagerConfig, log);

		return buildGraph(
			prior,
			jobGraph,
			jobManagerConfig,
			futureExecutor,
			ioExecutor,
			slotProvider,
			classLoader,
			recoveryFactory,
			rpcTimeout,
			restartStrategy,
			metrics,
			blobWriter,
			allocationTimeout,
			log,
			shuffleMaster,
			partitionTracker,
			failoverStrategy);
	}

	/**
	 * 建立ExecutionGraph
	 * @param prior
	 * @param jobGraph  用于构建ExecutionGraph使用的JobGraph对象
	 * @param jobManagerConfig JobMaster的配置信息
	 * @param futureExecutor ScheduledExecutorService实例，用于执行定时线程。
	 * @param ioExecutor
	 * @param slotProvider Slot资源提供者，实际上就是SlotPool实例
	 * @param classLoader  通过JAR包构建用户的ClassLoader
	 * @param recoveryFactory 用于恢复Checkpoint数据的工厂实现类。
	 * @param rpcTimeout
	 * @param restartStrategy 用于指定Task重启策略
	 * @param metrics 用于记录当前JobManager中与Job相关的MetricGroup
	 * @param blobWriter
	 * @param allocationTimeout
	 * @param log
	 * @param shuffleMaster 当前Job使用的ShuffleMaster管理类，用于注册和管理Shuffle中创建的ResultPartition信息。
	 * @param partitionTracker 用于监控和跟踪Job中所有Task的分区信息
	 * @param failoverStrategyFactory 获取任务容错配置的工厂类，主要用创建FailoverStrategy，在Scheduler中会通过FailoverStrategy控制Task的容器策略。
	 * @return
	 * @throws JobExecutionException
	 * @throws JobException
	 */
	public static ExecutionGraph buildGraph(
		@Nullable ExecutionGraph prior,
		JobGraph jobGraph,
		Configuration jobManagerConfig,
		ScheduledExecutorService futureExecutor,
		Executor ioExecutor,
		SlotProvider slotProvider,
		ClassLoader classLoader,
		CheckpointRecoveryFactory recoveryFactory,
		Time rpcTimeout,
		RestartStrategy restartStrategy,
		MetricGroup metrics,
		BlobWriter blobWriter,
		Time allocationTimeout,
		Logger log,
		ShuffleMaster<?> shuffleMaster,
		JobMasterPartitionTracker partitionTracker,
		FailoverStrategy.Factory failoverStrategyFactory) throws JobExecutionException, JobException {

		checkNotNull(jobGraph, "job graph cannot be null");

		final String jobName = jobGraph.getName();
		final JobID jobId = jobGraph.getJobID();

		final JobInformation jobInformation = new JobInformation(
			jobId,
			jobName,
			jobGraph.getSerializedExecutionConfig(),
			jobGraph.getJobConfiguration(),
			jobGraph.getUserJarBlobKeys(),
			jobGraph.getClasspaths());

		final int maxPriorAttemptsHistoryLength =
				jobManagerConfig.getInteger(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);

		final PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory =
			PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(jobManagerConfig);

		// create a new execution graph, if none exists so far
		final ExecutionGraph executionGraph;
		try {
			executionGraph = (prior != null) ? prior :
				new ExecutionGraph(
					jobInformation,
					futureExecutor,
					ioExecutor,
					rpcTimeout,
					restartStrategy,
					maxPriorAttemptsHistoryLength,
					failoverStrategyFactory,
					slotProvider,
					classLoader,
					blobWriter,
					allocationTimeout,
					partitionReleaseStrategyFactory,
					shuffleMaster,
					partitionTracker,
					jobGraph.getScheduleMode());
		} catch (IOException e) {
			throw new JobException("Could not create the ExecutionGraph.", e);
		}

		// set the basic properties

		try {
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
		}
		catch (Throwable t) {
			log.warn("Cannot create JSON plan for job", t);
			// give the graph an empty plan
			executionGraph.setJsonPlan("{}");
		}

		// initialize the vertices that have a master initialization hook
		// file output formats create directories here, input formats create splits

		final long initMasterStart = System.nanoTime();
		log.info("Running initialization on master for job {} ({}).", jobName, jobId);

		for (JobVertex vertex : jobGraph.getVertices()) {
			String executableClass = vertex.getInvokableClassName();
			if (executableClass == null || executableClass.isEmpty()) {
				throw new JobSubmissionException(jobId,
						"The vertex " + vertex.getID() + " (" + vertex.getName() + ") has no invokable class.");
			}

			try {
				vertex.initializeOnMaster(classLoader);
			}
			catch (Throwable t) {
					throw new JobExecutionException(jobId,
							"Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(), t);
			}
		}

		log.info("Successfully ran initialization on master in {} ms.",
				(System.nanoTime() - initMasterStart) / 1_000_000);

		// topologically sort the job vertices and attach the graph to the existing one
		// 对JobGraph中的JobVertex节点进行拓扑排序，得到List<JobVertex>
		List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
		if (log.isDebugEnabled()) {
			log.debug("Adding {} vertices from job graph {} ({}).", sortedTopology.size(), jobName, jobId);
		}
		/**
		 * 构建ExecutionGraph的核心方法
		 *
		 */
		executionGraph.attachJobGraph(sortedTopology);

		if (log.isDebugEnabled()) {
			log.debug("Successfully created execution graph from job graph {} ({}).", jobName, jobId);
		}
		/**
		 * 1. 根据snapshotSettings配置获取triggerVertices、ackVertices、confirmVertices节点集合，并转换对应的ExecutionJobVertex集合。其中triggerVertices集合
		 *    存储了所有SourceOperator节点，这些节点通过CheckpointCoordinator主动触发Checkpoint操作。ackVertices和confirmVertices集合存储了StreamGraph中的全部
		 *    节点，代表所有节点都需要返回Ack确认信息并确认Checkpoint执行成功。
		 * 2. 创建CompletedCheckpointStore组件，用于存储Checkpoint过程中的元数据，当对作业进行恢复操作时会在CompletedCheckpointStore中检索最新完成的Checkpoint元
		 *    数据信息，让后基于元数据信息恢复Checkpoint中存储的状态数据。CompletedCheckpointStore有两种实现，分别为ZooKeeperCompletedCheckpointStore和StandaloneCompletedCheckpointStore
		 * 3. 在CompletedCheckpointStore中通过maxNumberOfCheckpointsToRetain参数配置以及结合checkpointIdCounter计数器保证只会存储固定数量的CompletedCheckpoint。
		 * 4. 创建CheckpointStatsTracker实例，用于监控和追踪Checkpoint执行和更新的情况，包括Checkpoint执行的统计信息以及执行状况，WebUI中显示的Checkpoint监控数据主要
		 *    来自CheckpointStatsTracker。
		 * 5. 创建StateBackend，从UserClassLoader中反序列化出应用指定的StateBackend并设定为applicationConfiguredBackend。如果在应用中没有指定StateBackend，则调用
		 *    StateBackendLoader.fromApplicationOrConfigOrDefault()方法从系统默认配置中加载StateBackend实现类。
		 * 6. 初始化用户自定义的Checkpoint Hook函数，通过 snapshotSettings.getMasterHooks()方法获取SerializedValue<MasterTriggerRestoreHook.Factory[]>，并进行
		 *    线程上下文切换，以保证在UserClassLoader中正确创建并加载MasterTriggerRestoreHook.Factory的实现类。
		 * 7. 调用snapshotSettings.getCheckpointCoordinatorConfiguration()方法获取CheckpointCoordinatorConfiguration，最终调用executionGraph.enableCheckpointing()
		 *    方法，在作业的执行和调度过程中开启Checkpoint。
		 */
		// configure the state checkpointing
		// 从配置状态数据checkpointing，从jobGraph中获取JobCheckpointingSettings
		JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();
		// 如果snapshotSettings不为null，则开启checkpoint功能呢
		if (snapshotSettings != null) {
			List<ExecutionJobVertex> triggerVertices =
					idToVertex(snapshotSettings.getVerticesToTrigger(), executionGraph);

			List<ExecutionJobVertex> ackVertices =
					idToVertex(snapshotSettings.getVerticesToAcknowledge(), executionGraph);

			List<ExecutionJobVertex> confirmVertices =
					idToVertex(snapshotSettings.getVerticesToConfirm(), executionGraph);
			// 创建CompletedCheckpointStore
			CompletedCheckpointStore completedCheckpoints;
			CheckpointIDCounter checkpointIdCounter;
			try {
				int maxNumberOfCheckpointsToRetain = jobManagerConfig.getInteger(
						CheckpointingOptions.MAX_RETAINED_CHECKPOINTS);

				if (maxNumberOfCheckpointsToRetain <= 0) {
					// warning and use 1 as the default value if the setting in
					// state.checkpoints.max-retained-checkpoints is not greater than 0.
					log.warn("The setting for '{} : {}' is invalid. Using default value of {}",
							CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(),
							maxNumberOfCheckpointsToRetain,
							CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue());

					maxNumberOfCheckpointsToRetain = CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue();
				}
				// 通过recoveryFactory创建CheckpointStore
				completedCheckpoints = recoveryFactory.createCheckpointStore(jobId, maxNumberOfCheckpointsToRetain, classLoader);
				// 通过recoveryFactory创建CheckpointIDCounter
				checkpointIdCounter = recoveryFactory.createCheckpointIDCounter(jobId);
			}
			catch (Exception e) {
				throw new JobExecutionException(jobId, "Failed to initialize high-availability checkpoint handler", e);
			}

			// Maximum number of remembered checkpoints 获取checkpoints最长的记录次数
			int historySize = jobManagerConfig.getInteger(WebOptions.CHECKPOINTS_HISTORY_SIZE);
			// 创建CheckpointStatsTracker实例
			CheckpointStatsTracker checkpointStatsTracker = new CheckpointStatsTracker(
					historySize,
					ackVertices,
					snapshotSettings.getCheckpointCoordinatorConfiguration(),
					metrics);

			// load the state backend from the application settings
			// 从Application中获取StateBackend
			final StateBackend applicationConfiguredBackend;
			final SerializedValue<StateBackend> serializedAppConfigured = snapshotSettings.getDefaultStateBackend();

			if (serializedAppConfigured == null) {
				applicationConfiguredBackend = null;
			}
			else {
				try {
					applicationConfiguredBackend = serializedAppConfigured.deserializeValue(classLoader);
				} catch (IOException | ClassNotFoundException e) {
					throw new JobExecutionException(jobId,
							"Could not deserialize application-defined state backend.", e);
				}
			}
			// 获取最终的rootBackend
			final StateBackend rootBackend;
			try {
				rootBackend = StateBackendLoader.fromApplicationOrConfigOrDefault(
						applicationConfiguredBackend, jobManagerConfig, classLoader, log);
			}
			catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
				throw new JobExecutionException(jobId, "Could not instantiate configured state backend", e);
			}

			// instantiate the user-defined checkpoint hooks
			// 初始化用户自定义的checkpoint Hooks函数
			final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks = snapshotSettings.getMasterHooks();
			final List<MasterTriggerRestoreHook<?>> hooks;
			// 如果serializedHooks为空，则hooks为空
			if (serializedHooks == null) {
				hooks = Collections.emptyList();
			}
			else {
				// 加载MasterTriggerRestoreHook
				final MasterTriggerRestoreHook.Factory[] hookFactories;
				try {
					hookFactories = serializedHooks.deserializeValue(classLoader);
				}
				catch (IOException | ClassNotFoundException e) {
					throw new JobExecutionException(jobId, "Could not instantiate user-defined checkpoint hooks", e);
				}
				// 设定ClassLoader为UserClassLoader
				final Thread thread = Thread.currentThread();
				final ClassLoader originalClassLoader = thread.getContextClassLoader();
				thread.setContextClassLoader(classLoader);
				// 创建hooks函数
				try {
					hooks = new ArrayList<>(hookFactories.length);
					for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
						hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
					}
				}
				// 将thread的ContextClassLoader设定为originalClassLoader
				finally {
					thread.setContextClassLoader(originalClassLoader);
				}
			}
			// 获取CheckpointCoordinatorConfiguration
			final CheckpointCoordinatorConfiguration chkConfig = snapshotSettings.getCheckpointCoordinatorConfiguration();
			// 开启executionGraph中的CheckPoint功能
			executionGraph.enableCheckpointing(
				chkConfig,
				triggerVertices,
				ackVertices,
				confirmVertices,
				hooks,
				checkpointIdCounter,
				completedCheckpoints,
				rootBackend,
				checkpointStatsTracker);
		}

		// create all the metrics for the Execution Graph

		metrics.gauge(RestartTimeGauge.METRIC_NAME, new RestartTimeGauge(executionGraph));
		metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(executionGraph));
		metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(executionGraph));

		executionGraph.getFailoverStrategy().registerMetrics(metrics);

		return executionGraph;
	}

	private static List<ExecutionJobVertex> idToVertex(
			List<JobVertexID> jobVertices, ExecutionGraph executionGraph) throws IllegalArgumentException {

		List<ExecutionJobVertex> result = new ArrayList<>(jobVertices.size());

		for (JobVertexID id : jobVertices) {
			ExecutionJobVertex vertex = executionGraph.getJobVertex(id);
			if (vertex != null) {
				result.add(vertex);
			} else {
				throw new IllegalArgumentException(
						"The snapshot checkpointing settings refer to non-existent vertex " + id);
			}
		}

		return result;
	}

	// ------------------------------------------------------------------------

	/** This class is not supposed to be instantiated. */
	private ExecutionGraphBuilder() {}
}
