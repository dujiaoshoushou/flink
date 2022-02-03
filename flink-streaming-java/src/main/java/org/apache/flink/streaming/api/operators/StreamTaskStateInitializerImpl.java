/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.Preconditions;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * This class is the main implementation of a {@link StreamTaskStateInitializer}. This class obtains the state to create
 * {@link StreamOperatorStateContext} objects for stream operators from the {@link TaskStateManager} of the task that
 * runs the stream task and hence the operator.
 *
 * <p>This implementation operates on top a {@link TaskStateManager}, from which it receives everything required to
 * restore state in the backends from checkpoints or savepoints.
 */
public class StreamTaskStateInitializerImpl implements StreamTaskStateInitializer {

	/** The logger for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(StreamTaskStateInitializerImpl.class);

	/**
	 * The environment of the task. This is required as parameter to construct state backends via their factory.
	 */
	private final Environment environment;

	/** The state manager of the tasks provides the information used to restore potential previous state. */
	private final TaskStateManager taskStateManager;

	/** This object is the factory for everything related to state backends and checkpointing. */
	private final StateBackend stateBackend;

	public StreamTaskStateInitializerImpl(
		Environment environment,
		StateBackend stateBackend) {

		this.environment = environment;
		this.taskStateManager = Preconditions.checkNotNull(environment.getTaskStateManager());
		this.stateBackend = Preconditions.checkNotNull(stateBackend);
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 *
	 * @param operatorID the id of the operator for which the context is created. Cannot be null.
	 * @param operatorClassName the classname of the operator instance for which the context is created. Cannot be null.
	 * @param processingTimeService
	 * @param keyContext the key context of the operator instance for which the context is created Cannot be null.
	 * @param keySerializer the key-serializer for the operator. Can be null.
	 * @param streamTaskCloseableRegistry the closeable registry to which created closeable objects will be registered.
	 * @param metricGroup the parent metric group for all statebackend metrics
	 * @return
	 * @throws Exception
	 * 1. 从environment中获取TaskInfo，并基于Task实例创建OperatorSubtaskDescriptionText。Operator中Task实例的描述信息包含OperatorID、
	 *    OperatorClassName等，最终用于创建OperatorStateBackend的状态存储后端。
	 * 2. 调用keyedStateBackend()方法创建KeyedStateBackend、KeyedStateBackend是KeyedState的状态管理后端，提供创建和管理KeyedState的方法。
	 * 3. 调用operatorStateBackend()方法创建OperatorStateBackend，OperatorStateBackend是OperatorState的状态管理后端，提供获取和管理OpeatorState的接口。
	 * 4. 调用rawKeyedStateInputs()方法创建KeyGroupStatePartitionStreamProvider实例，该实例提供创建和获取原生KeyedState的方法。
	 * 5. 调用rawOperatorStateInputs()方法创建StatePartitionStreamProvider实例，该实例提供创建和获取原生OperatorState的方法。
	 * 6. 将所有创建出来的托管状态管理后端keyedStateBackend和OperatorStateBackend、原生状态存储后端rawKeyedStateInputs和rawOperatorStateInputs
	 *    及timeServiceManager实例，全部封装在StreamOperatorStateContextImpl上下文对象中，并返回给AbstractStreamOperator使用。
	 */
	@Override
	public StreamOperatorStateContext streamOperatorStateContext(
		@Nonnull OperatorID operatorID,
		@Nonnull String operatorClassName,
		@Nonnull ProcessingTimeService processingTimeService,
		@Nonnull KeyContext keyContext,
		@Nullable TypeSerializer<?> keySerializer,
		@Nonnull CloseableRegistry streamTaskCloseableRegistry,
		@Nonnull MetricGroup metricGroup) throws Exception {
		// 获取Task实例信息
		TaskInfo taskInfo = environment.getTaskInfo();
		OperatorSubtaskDescriptionText operatorSubtaskDescription =
			new OperatorSubtaskDescriptionText(
				operatorID,
				operatorClassName,
				taskInfo.getIndexOfThisSubtask(),
				taskInfo.getNumberOfParallelSubtasks());

		final String operatorIdentifierText = operatorSubtaskDescription.toString();

		final PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates =
			taskStateManager.prioritizedOperatorState(operatorID);

		AbstractKeyedStateBackend<?> keyedStatedBackend = null;
		OperatorStateBackend operatorStateBackend = null;
		CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs = null;
		CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs = null;
		InternalTimeServiceManager<?> timeServiceManager;

		try {

			// -------------- Keyed State Backend --------------
			// 创建keyed类型状态后端
			keyedStatedBackend = keyedStatedBackend(
				keySerializer,
				operatorIdentifierText,
				prioritizedOperatorSubtaskStates,
				streamTaskCloseableRegistry,
				metricGroup);

			// -------------- Operator State Backend --------------
			// 创建Operator类型状态后端
			operatorStateBackend = operatorStateBackend(
				operatorIdentifierText,
				prioritizedOperatorSubtaskStates,
				streamTaskCloseableRegistry);

			// -------------- Raw State Streams --------------
			// 创建原生类型状态后端
			rawKeyedStateInputs = rawKeyedStateInputs(
				prioritizedOperatorSubtaskStates.getPrioritizedRawKeyedState().iterator());
			streamTaskCloseableRegistry.registerCloseable(rawKeyedStateInputs);

			rawOperatorStateInputs = rawOperatorStateInputs(
				prioritizedOperatorSubtaskStates.getPrioritizedRawOperatorState().iterator());
			streamTaskCloseableRegistry.registerCloseable(rawOperatorStateInputs);

			// -------------- Internal Timer Service Manager --------------
			// 创建 Internal Timer Service Manager实例
			timeServiceManager = internalTimeServiceManager(keyedStatedBackend, keyContext, processingTimeService, rawKeyedStateInputs);

			// -------------- Preparing return value --------------
			// 创建 StreamOperatorStateContext实现类
			return new StreamOperatorStateContextImpl(
				prioritizedOperatorSubtaskStates.isRestored(),
				operatorStateBackend,
				keyedStatedBackend,
				timeServiceManager,
				rawOperatorStateInputs,
				rawKeyedStateInputs);
		} catch (Exception ex) {

			// cleanup if something went wrong before results got published.
			if (keyedStatedBackend != null) {
				if (streamTaskCloseableRegistry.unregisterCloseable(keyedStatedBackend)) {
					IOUtils.closeQuietly(keyedStatedBackend);
				}
				// release resource (e.g native resource)
				keyedStatedBackend.dispose();
			}

			if (operatorStateBackend != null) {
				if (streamTaskCloseableRegistry.unregisterCloseable(operatorStateBackend)) {
					IOUtils.closeQuietly(operatorStateBackend);
				}
				operatorStateBackend.dispose();
			}

			if (streamTaskCloseableRegistry.unregisterCloseable(rawKeyedStateInputs)) {
				IOUtils.closeQuietly(rawKeyedStateInputs);
			}

			if (streamTaskCloseableRegistry.unregisterCloseable(rawOperatorStateInputs)) {
				IOUtils.closeQuietly(rawOperatorStateInputs);
			}

			throw new Exception("Exception while creating StreamOperatorStateContext.", ex);
		}
	}

	protected <K> InternalTimeServiceManager<K> internalTimeServiceManager(
		AbstractKeyedStateBackend<K> keyedStatedBackend,
		KeyContext keyContext, //the operator
		ProcessingTimeService processingTimeService,
		Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStates) throws Exception {

		if (keyedStatedBackend == null) {
			return null;
		}

		final KeyGroupRange keyGroupRange = keyedStatedBackend.getKeyGroupRange();

		final InternalTimeServiceManager<K> timeServiceManager = new InternalTimeServiceManager<>(
			keyGroupRange,
			keyContext,
			keyedStatedBackend,
			processingTimeService,
			keyedStatedBackend.requiresLegacySynchronousTimerSnapshots());

		// and then initialize the timer services
		for (KeyGroupStatePartitionStreamProvider streamProvider : rawKeyedStates) {
			int keyGroupIdx = streamProvider.getKeyGroupId();

			Preconditions.checkArgument(keyGroupRange.contains(keyGroupIdx),
				"Key Group " + keyGroupIdx + " does not belong to the local range.");

			timeServiceManager.restoreStateForKeyGroup(
				streamProvider.getStream(),
				keyGroupIdx, environment.getUserClassLoader());
		}

		return timeServiceManager;
	}

	/**
	 * 1. 创建cancelStreamRegistryForRestore对应的CloseableRegistry并注册到backendCloseableRegistry中，确保在任务取消的情况下能够关闭在恢复状态时构造的数据流。
	 * 2. 创建OperatorStateBackend对应的BackendRestorerProcedure，OperatorStateBackend中封装了stateBackend.createOperatorStateBackend()方法，并包含了
	 *    恢复历史状态数据的操作。
	 * 3. 调用 backendRestorer.createAndRestore()方法创建OperatorStateBackend，并恢复状态数据。其中prioritizedOperatorSubtaskStates是从TaskManager中根据
	 *    OperatorID获取的算子专有历史状态，可以通过prioritizedOperatorSubtaskStates获取当前算子中的PrioritizedManagedOperatorState并基于状态数据恢复OperatorStateBackend
	 *    中算子的状态。
	 */
	protected OperatorStateBackend operatorStateBackend(
		String operatorIdentifierText,
		PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates,
		CloseableRegistry backendCloseableRegistry) throws Exception {

		String logDescription = "operator state backend for " + operatorIdentifierText;

		// Now restore processing is included in backend building/constructing process, so we need to make sure
		// each stream constructed in restore could also be closed in case of task cancel, for example the data
		// input stream opened for serDe during restore.
		CloseableRegistry cancelStreamRegistryForRestore = new CloseableRegistry();
		backendCloseableRegistry.registerCloseable(cancelStreamRegistryForRestore);
		BackendRestorerProcedure<OperatorStateBackend, OperatorStateHandle> backendRestorer =
			new BackendRestorerProcedure<>(
				(stateHandles) -> stateBackend.createOperatorStateBackend(
					environment,
					operatorIdentifierText,
					stateHandles,
					cancelStreamRegistryForRestore),
				backendCloseableRegistry,
				logDescription);

		try {
			return backendRestorer.createAndRestore(
				prioritizedOperatorSubtaskStates.getPrioritizedManagedOperatorState());
		} finally {
			if (backendCloseableRegistry.unregisterCloseable(cancelStreamRegistryForRestore)) {
				IOUtils.closeQuietly(cancelStreamRegistryForRestore);
			}
		}
	}

	/**
	 * 1. 从environment参数中获取当前Task的TaskInfo，然后从TaskInfo中获取maxNumberOfParallelSubtasks、numberOfParallelSubtasks等参数信息，
	 *    并基于参数创建KeyGroupRange，用于表示当前Task实例中存储的Key分组区间。
	 * 2. 创建cancelStreamRegistryForRestore对应的CloseableRegistry并注册到backendCloseableRegistry中，用于确保在任务取消的情况下关闭在恢复状态
	 *    过程中构造的数据流。
	 * 3. 创建KeyeStateBackend对应的BackendRestorerProcedure，在BackendRestorerProcedure中封装了stateBackend.createKeyedStateBackend()方法，
	 *    也包含恢复历史状态数据的方法。
	 * 4. 调用backendRestorer.createAndRestore()方法创建KeyedStateBackend，同时对状态数据进行恢复。prioritizedOperatorSubtaskStates是从TaskStteManager中
	 *    根据OperatorID获取的算子历史状态，可以通过prioritizedOperatorSubtaskStates获取当前算子的prioritizedManagedKeyedState，并基于这些状态数据恢复算子的状态。
	 */
	protected <K> AbstractKeyedStateBackend<K> keyedStatedBackend(
		TypeSerializer<K> keySerializer,
		String operatorIdentifierText,
		PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates,
		CloseableRegistry backendCloseableRegistry,
		MetricGroup metricGroup) throws Exception {

		if (keySerializer == null) {
			return null;
		}

		String logDescription = "keyed state backend for " + operatorIdentifierText;

		TaskInfo taskInfo = environment.getTaskInfo();

		final KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
			taskInfo.getMaxNumberOfParallelSubtasks(),
			taskInfo.getNumberOfParallelSubtasks(),
			taskInfo.getIndexOfThisSubtask());

		// Now restore processing is included in backend building/constructing process, so we need to make sure
		// each stream constructed in restore could also be closed in case of task cancel, for example the data
		// input stream opened for serDe during restore.
		// 确保恢复状态过程中构建的数据流被关闭
		CloseableRegistry cancelStreamRegistryForRestore = new CloseableRegistry();
		backendCloseableRegistry.registerCloseable(cancelStreamRegistryForRestore);
		// 创建BackendRestorerProcedure
		BackendRestorerProcedure<AbstractKeyedStateBackend<K>, KeyedStateHandle> backendRestorer =
			new BackendRestorerProcedure<>(
				(stateHandles) -> stateBackend.createKeyedStateBackend(
					environment,
					environment.getJobID(),
					operatorIdentifierText,
					keySerializer,
					taskInfo.getMaxNumberOfParallelSubtasks(),
					keyGroupRange,
					environment.getTaskKvStateRegistry(),
					TtlTimeProvider.DEFAULT,
					metricGroup,
					stateHandles,
					cancelStreamRegistryForRestore),
				backendCloseableRegistry,
				logDescription);

		try {
			return backendRestorer.createAndRestore(
				prioritizedOperatorSubtaskStates.getPrioritizedManagedKeyedState());
		} finally {
			if (backendCloseableRegistry.unregisterCloseable(cancelStreamRegistryForRestore)) {
				IOUtils.closeQuietly(cancelStreamRegistryForRestore);
			}
		}
	}

	protected CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs(
		Iterator<StateObjectCollection<OperatorStateHandle>> restoreStateAlternatives) {

		if (restoreStateAlternatives.hasNext()) {

			Collection<OperatorStateHandle> rawOperatorState = restoreStateAlternatives.next();
			// TODO currently this does not support local state recovery, so we expect there is only one handle.
			Preconditions.checkState(
				!restoreStateAlternatives.hasNext(),
				"Local recovery is currently not implemented for raw operator state, but found state alternative.");

			if (rawOperatorState != null) {
				return new CloseableIterable<StatePartitionStreamProvider>() {

					final CloseableRegistry closeableRegistry = new CloseableRegistry();

					@Override
					public void close() throws IOException {
						closeableRegistry.close();
					}

					@Nonnull
					@Override
					public Iterator<StatePartitionStreamProvider> iterator() {
						return new OperatorStateStreamIterator(
							DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
							rawOperatorState.iterator(), closeableRegistry);
					}
				};
			}
		}

		return CloseableIterable.empty();
	}

	protected CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs(
		Iterator<StateObjectCollection<KeyedStateHandle>> restoreStateAlternatives) {

		if (restoreStateAlternatives.hasNext()) {
			Collection<KeyedStateHandle> rawKeyedState = restoreStateAlternatives.next();

			// TODO currently this does not support local state recovery, so we expect there is only one handle.
			Preconditions.checkState(
				!restoreStateAlternatives.hasNext(),
				"Local recovery is currently not implemented for raw keyed state, but found state alternative.");

			if (rawKeyedState != null) {
				Collection<KeyGroupsStateHandle> keyGroupsStateHandles = transform(rawKeyedState);
				final CloseableRegistry closeableRegistry = new CloseableRegistry();

				return new CloseableIterable<KeyGroupStatePartitionStreamProvider>() {
					@Override
					public void close() throws IOException {
						closeableRegistry.close();
					}

					@Override
					public Iterator<KeyGroupStatePartitionStreamProvider> iterator() {
						return new KeyGroupStreamIterator(keyGroupsStateHandles.iterator(), closeableRegistry);
					}
				};
			}
		}

		return CloseableIterable.empty();
	}

	// =================================================================================================================

	private static class KeyGroupStreamIterator
		extends AbstractStateStreamIterator<KeyGroupStatePartitionStreamProvider, KeyGroupsStateHandle> {

		private Iterator<Tuple2<Integer, Long>> currentOffsetsIterator;

		KeyGroupStreamIterator(
			Iterator<KeyGroupsStateHandle> stateHandleIterator, CloseableRegistry closableRegistry) {

			super(stateHandleIterator, closableRegistry);
		}

		@Override
		public boolean hasNext() {

			if (null != currentStateHandle && currentOffsetsIterator.hasNext()) {

				return true;
			}

			closeCurrentStream();

			while (stateHandleIterator.hasNext()) {
				currentStateHandle = stateHandleIterator.next();
				if (currentStateHandle.getKeyGroupRange().getNumberOfKeyGroups() > 0) {
					currentOffsetsIterator = currentStateHandle.getGroupRangeOffsets().iterator();

					return true;
				}
			}

			return false;
		}

		@Override
		public KeyGroupStatePartitionStreamProvider next() {

			if (!hasNext()) {

				throw new NoSuchElementException("Iterator exhausted");
			}

			Tuple2<Integer, Long> keyGroupOffset = currentOffsetsIterator.next();
			try {
				if (null == currentStream) {
					openCurrentStream();
				}

				currentStream.seek(keyGroupOffset.f1);
				return new KeyGroupStatePartitionStreamProvider(currentStream, keyGroupOffset.f0);

			} catch (IOException ioex) {
				return new KeyGroupStatePartitionStreamProvider(ioex, keyGroupOffset.f0);
			}
		}
	}

	private static class OperatorStateStreamIterator
		extends AbstractStateStreamIterator<StatePartitionStreamProvider, OperatorStateHandle> {

		private final String stateName; //TODO since we only support a single named state in raw, this could be dropped
		private long[] offsets;
		private int offPos;

		OperatorStateStreamIterator(
			String stateName,
			Iterator<OperatorStateHandle> stateHandleIterator,
			CloseableRegistry closableRegistry) {

			super(stateHandleIterator, closableRegistry);
			this.stateName = Preconditions.checkNotNull(stateName);
		}

		@Override
		public boolean hasNext() {

			if (null != offsets && offPos < offsets.length) {

				return true;
			}

			closeCurrentStream();

			while (stateHandleIterator.hasNext()) {
				currentStateHandle = stateHandleIterator.next();
				OperatorStateHandle.StateMetaInfo metaInfo =
					currentStateHandle.getStateNameToPartitionOffsets().get(stateName);

				if (null != metaInfo) {
					long[] metaOffsets = metaInfo.getOffsets();
					if (null != metaOffsets && metaOffsets.length > 0) {
						this.offsets = metaOffsets;
						this.offPos = 0;

						if (closableRegistry.unregisterCloseable(currentStream)) {
							IOUtils.closeQuietly(currentStream);
							currentStream = null;
						}

						return true;
					}
				}
			}

			return false;
		}

		@Override
		public StatePartitionStreamProvider next() {

			if (!hasNext()) {

				throw new NoSuchElementException("Iterator exhausted");
			}

			long offset = offsets[offPos++];

			try {
				if (null == currentStream) {
					openCurrentStream();
				}

				currentStream.seek(offset);
				return new StatePartitionStreamProvider(currentStream);

			} catch (IOException ioex) {
				return new StatePartitionStreamProvider(ioex);
			}
		}
	}

	private abstract static class AbstractStateStreamIterator<
		T extends StatePartitionStreamProvider, H extends StreamStateHandle>
		implements Iterator<T> {

		protected final Iterator<H> stateHandleIterator;
		protected final CloseableRegistry closableRegistry;

		protected H currentStateHandle;
		protected FSDataInputStream currentStream;

		AbstractStateStreamIterator(
			Iterator<H> stateHandleIterator,
			CloseableRegistry closableRegistry) {

			this.stateHandleIterator = Preconditions.checkNotNull(stateHandleIterator);
			this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
		}

		protected void openCurrentStream() throws IOException {

			Preconditions.checkState(currentStream == null);

			FSDataInputStream stream = currentStateHandle.openInputStream();
			closableRegistry.registerCloseable(stream);
			currentStream = stream;
		}

		protected void closeCurrentStream() {
			if (closableRegistry.unregisterCloseable(currentStream)) {
				IOUtils.closeQuietly(currentStream);
			}
			currentStream = null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Read only Iterator");
		}
	}

	private static Collection<KeyGroupsStateHandle> transform(Collection<KeyedStateHandle> keyedStateHandles) {

		if (keyedStateHandles == null) {
			return null;
		}

		List<KeyGroupsStateHandle> keyGroupsStateHandles = new ArrayList<>(keyedStateHandles.size());

		for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {

			if (keyedStateHandle instanceof KeyGroupsStateHandle) {
				keyGroupsStateHandles.add((KeyGroupsStateHandle) keyedStateHandle);
			} else if (keyedStateHandle != null) {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected: " + KeyGroupsStateHandle.class +
					", but found: " + keyedStateHandle.getClass() + ".");
			}
		}

		return keyGroupsStateHandles;
	}

	private static class StreamOperatorStateContextImpl implements StreamOperatorStateContext {

		private final boolean restored;

		private final OperatorStateBackend operatorStateBackend;
		private final AbstractKeyedStateBackend<?> keyedStateBackend;
		private final InternalTimeServiceManager<?> internalTimeServiceManager;

		private final CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs;
		private final CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs;

		StreamOperatorStateContextImpl(
			boolean restored,
			OperatorStateBackend operatorStateBackend,
			AbstractKeyedStateBackend<?> keyedStateBackend,
			InternalTimeServiceManager<?> internalTimeServiceManager,
			CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs,
			CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs) {

			this.restored = restored;
			this.operatorStateBackend = operatorStateBackend;
			this.keyedStateBackend = keyedStateBackend;
			this.internalTimeServiceManager = internalTimeServiceManager;
			this.rawOperatorStateInputs = rawOperatorStateInputs;
			this.rawKeyedStateInputs = rawKeyedStateInputs;
		}

		@Override
		public boolean isRestored() {
			return restored;
		}

		@Override
		public AbstractKeyedStateBackend<?> keyedStateBackend() {
			return keyedStateBackend;
		}

		@Override
		public OperatorStateBackend operatorStateBackend() {
			return operatorStateBackend;
		}

		@Override
		public InternalTimeServiceManager<?> internalTimerServiceManager() {
			return internalTimeServiceManager;
		}

		@Override
		public CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs() {
			return rawOperatorStateInputs;
		}

		@Override
		public CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs() {
			return rawKeyedStateInputs;
		}
	}

}
