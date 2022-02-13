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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferListener.NotificationResult;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A buffer pool used to manage a number of {@link Buffer} instances from the
 * {@link NetworkBufferPool}.
 *
 * <p>Buffer requests are mediated to the network buffer pool to ensure dead-lock
 * free operation of the network stack by limiting the number of buffers per
 * local buffer pool. It also implements the default mechanism for buffer
 * recycling, which ensures that every buffer is ultimately returned to the
 * network buffer pool.
 *
 * <p>The size of this pool can be dynamically changed at runtime ({@link #setNumBuffers(int)}. It
 * will then lazily return the required number of buffers to the {@link NetworkBufferPool} to
 * match its new size.
 */
class LocalBufferPool implements BufferPool {
	private static final Logger LOG = LoggerFactory.getLogger(LocalBufferPool.class);

	/** Global network buffer pool to get buffers from. */
	private final NetworkBufferPool networkBufferPool;

	/** The minimum number of required segments for this pool. */
	private final int numberOfRequiredMemorySegments;

	/**
	 * The currently available memory segments. These are segments, which have been requested from
	 * the network buffer pool and are currently not handed out as Buffer instances.
	 *
	 * <p><strong>BEWARE:</strong> Take special care with the interactions between this lock and
	 * locks acquired before entering this class vs. locks being acquired during calls to external
	 * code inside this class, e.g. with
	 * {@link org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel#bufferQueue}
	 * via the {@link #registeredListeners} callback.
	 */
	private final ArrayDeque<MemorySegment> availableMemorySegments = new ArrayDeque<MemorySegment>();

	/**
	 * Buffer availability listeners, which need to be notified when a Buffer becomes available.
	 * Listeners can only be registered at a time/state where no Buffer instance was available.
	 */
	private final ArrayDeque<BufferListener> registeredListeners = new ArrayDeque<>();

	/** Maximum number of network buffers to allocate. */
	private final int maxNumberOfMemorySegments;

	/** The current size of this pool. */
	private int currentPoolSize;

	/**
	 * Number of all memory segments, which have been requested from the network buffer pool and are
	 * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available segments).
	 */
	private int numberOfRequestedMemorySegments;

	private boolean isDestroyed;

	@Nullable
	private final BufferPoolOwner bufferPoolOwner;

	private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

	/**
	 * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal number of
	 * network buffers being available.
	 *
	 * @param networkBufferPool
	 * 		global network buffer pool to get buffers from
	 * @param numberOfRequiredMemorySegments
	 * 		minimum number of network buffers
	 */
	LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments) {
		this(networkBufferPool, numberOfRequiredMemorySegments, Integer.MAX_VALUE, null);
	}

	/**
	 * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal and maximal
	 * number of network buffers being available.
	 *
	 * @param networkBufferPool
	 * 		global network buffer pool to get buffers from
	 * @param numberOfRequiredMemorySegments
	 * 		minimum number of network buffers
	 * @param maxNumberOfMemorySegments
	 * 		maximum number of network buffers to allocate
	 */
	LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments,
			int maxNumberOfMemorySegments) {
		this(networkBufferPool, numberOfRequiredMemorySegments, maxNumberOfMemorySegments, null);
	}

	/**
	 * Local buffer pool based on the given <tt>networkBufferPool</tt> and <tt>bufferPoolOwner</tt>
	 * with a minimal and maximal number of network buffers being available.
	 *
	 * @param networkBufferPool
	 * 		global network buffer pool to get buffers from
	 * @param numberOfRequiredMemorySegments
	 * 		minimum number of network buffers
	 * @param maxNumberOfMemorySegments
	 * 		maximum number of network buffers to allocate
	 * 	@param bufferPoolOwner
	 * 		the owner of this buffer pool to release memory when needed
	 */
	LocalBufferPool(
		NetworkBufferPool networkBufferPool,
		int numberOfRequiredMemorySegments,
		int maxNumberOfMemorySegments,
		@Nullable BufferPoolOwner bufferPoolOwner) {
		checkArgument(maxNumberOfMemorySegments >= numberOfRequiredMemorySegments,
			"Maximum number of memory segments (%s) should not be smaller than minimum (%s).",
			maxNumberOfMemorySegments, numberOfRequiredMemorySegments);

		checkArgument(maxNumberOfMemorySegments > 0,
			"Maximum number of memory segments (%s) should be larger than 0.",
			maxNumberOfMemorySegments);

		LOG.debug("Using a local buffer pool with {}-{} buffers",
			numberOfRequiredMemorySegments, maxNumberOfMemorySegments);

		this.networkBufferPool = networkBufferPool;
		this.numberOfRequiredMemorySegments = numberOfRequiredMemorySegments;
		this.currentPoolSize = numberOfRequiredMemorySegments;
		this.maxNumberOfMemorySegments = maxNumberOfMemorySegments;
		this.bufferPoolOwner = bufferPoolOwner;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	@Override
	public boolean isDestroyed() {
		synchronized (availableMemorySegments) {
			return isDestroyed;
		}
	}

	@Override
	public int getNumberOfRequiredMemorySegments() {
		return numberOfRequiredMemorySegments;
	}

	@Override
	public int getMaxNumberOfMemorySegments() {
		return maxNumberOfMemorySegments;
	}

	@Override
	public int getNumberOfAvailableMemorySegments() {
		synchronized (availableMemorySegments) {
			return availableMemorySegments.size();
		}
	}

	@Override
	public int getNumBuffers() {
		synchronized (availableMemorySegments) {
			return currentPoolSize;
		}
	}

	@Override
	public int bestEffortGetNumOfUsedBuffers() {
		return Math.max(0, numberOfRequestedMemorySegments - availableMemorySegments.size());
	}

	@Override
	public Buffer requestBuffer() throws IOException {
		return toBuffer(requestMemorySegment());
	}

	@Override
	public BufferBuilder requestBufferBuilderBlocking() throws IOException, InterruptedException {
		return toBufferBuilder(requestMemorySegmentBlocking());
	}

	private Buffer toBuffer(MemorySegment memorySegment) {
		if (memorySegment == null) {
			return null;
		}
		return new NetworkBuffer(memorySegment, this);
	}

	private BufferBuilder toBufferBuilder(MemorySegment memorySegment) {
		if (memorySegment == null) {
			return null;
		}
		return new BufferBuilder(memorySegment, this);
	}

	private MemorySegment requestMemorySegmentBlocking() throws InterruptedException, IOException {
		MemorySegment segment;
		while ((segment = requestMemorySegment()) == null) {
			try {
				// wait until available
				getAvailableFuture().get();
			} catch (ExecutionException e) {
				LOG.error("The available future is completed exceptionally.", e);
				ExceptionUtils.rethrow(e);
			}
		}
		return segment;
	}

	/**
	 * 1. 调用ExcessMemorySegments释放额外申请的MemorySegment资源，遍历availableMemorySegments中的MemorySegment，通过MemorySegment是否为空判定
	 *    MemorySegment是否已经不再使用，直到申请的MemorySegments数量小于LocalBufferPool的容量。
	 * 2. 如果LocalBufferPool中的availableMemorySegments队列为空，LocalBufferPool直接调用requestMemorySegmentFromGlobal()方法，向NetworkBufferPool
	 *    申请更多MemorySegment资源。
	 * 3. 如果申请到的内存块为空，则继续到availableMemorySegments集合中执行拉取操作。如果还是没有获取MemorySegment，则调用availabilityHelper.resetUnavailable()
	 *    方法，将当前的LocalBufferPool置为不可用。
	 * 4. 如果成功申请到MemorySegment，则返回内存块，对于InputChannel来讲，会基于申请到的MemorySegment创建Buffer，而ResultPartition则利用MemorySegment内存块
	 *    创建BufferBuilder对象。
	 */
	@Nullable
	private MemorySegment requestMemorySegment() throws IOException {
		MemorySegment segment = null;
		synchronized (availableMemorySegments) {
			returnExcessMemorySegments();

			if (availableMemorySegments.isEmpty()) {
				segment = requestMemorySegmentFromGlobal();
			}
			// segment may have been released by buffer pool owner
			if (segment == null) {
				segment = availableMemorySegments.poll();
			}
			if (segment == null) {
				availabilityHelper.resetUnavailable();
			}
		}
		return segment;
	}

	/**
	 * 申请全局内存块
	 * 1. 通过numberOfRequestedMemorySegments < currentPoolSize添加判断需要申请的MemorySegment数量是否小于当前LocalBufferPool的容量，
	 *    如果超过则不予申请。
	 * 2. 如果申请的MemorySegment在LocalBufferPool容量范围内，则LocalBufferPool会调用networkBufferPool.requestMemorySegment()方法向NetworkBufferPool
	 *    申请MemorySegment资源。申请完毕后对numberOfRequestedMemorySegments进行累加操作，NetworkBufferPool会将申请到的MemorySegment返回给LocalBufferPool。
	 * 3. 如果申请到的MemorySegment数量超过了LocalBufferPool容量范围，同时bufferPoolOwner不为空，则调用bufferPoolOwner.releaseMemory(1)方法释放内部持有的
	 *    MemorySegment内存空间，如果释放成功，就会将MemorySegment放到LocalBufferPool中使用。
	 */
	@Nullable
	private MemorySegment requestMemorySegmentFromGlobal() throws IOException {
		assert Thread.holdsLock(availableMemorySegments);

		if (isDestroyed) {
			throw new IllegalStateException("Buffer pool is destroyed.");
		}

		if (numberOfRequestedMemorySegments < currentPoolSize) {
			final MemorySegment segment = networkBufferPool.requestMemorySegment();
			if (segment != null) {
				numberOfRequestedMemorySegments++;
				return segment;
			}
		}

		if (bufferPoolOwner != null) {
			bufferPoolOwner.releaseMemory(1);
		}

		return null;
	}

	/**
	 *
	 * 1. 开启循环，结束条件是NotificationResult的状态为BUFFER_USED。
	 * 2. 对availableMemorySegments进行加锁处理，判断LocalBufferPool是否被销毁以及申请到的MemorySegments数量是否大于LocalBufferPool的容量，
	 *    如果满足其中任何一个条件中，则直接调用returnMemorySegment(segment)方法回收MemorySegment，也就是返回给NetworkBufferPool。
	 * 3. 否则从注册的registeredListeners中获取BufferListener，这里的BufferListener实现类主要有RemoteInputChannel，用于监听Buffer的使用状况。
	 * 4. 如果listener为空，表明没有BufferListener监听Buffer的使用情况，则直接将内存块添加到availableMemorySegments集合中。
	 * 5. 如果availableMemorySegments.isEmpty()为True，表明此时LocalBufferPool处于不可用状态，因为没有可用的MemorySegments空间存储数据，
	 *    所以添加新的内存块后，会调用availabilityHelper.getUnavailableToResetAvailable()方法将LocalBufferPool转换为可用状态，否则availableMemorySegments
	 *    一直为空，此时无法再申请Buffer资源空间，也就不能对外提供Buffer申请服务。
	 * 6. 如果listener不为空，会通知listener当前内存块处于可用状态，其他的RemoteInputChannel会收到该内存块的资源，并转换为Buffer存储在本地FloatingBuffer中。
	 */
	@Override
	public void recycle(MemorySegment segment) {
		BufferListener listener;
		CompletableFuture<?> toNotify = null;
		NotificationResult notificationResult = NotificationResult.BUFFER_NOT_USED;
		// 直到LocalBufferPool是否被销毁以及申请到的MemorySegments数量是否大于LocalBufferPool的容量
		while (!notificationResult.isBufferUsed()) {
			synchronized (availableMemorySegments) {
				if (isDestroyed || numberOfRequestedMemorySegments > currentPoolSize) {
					// 如果符合条件，则将Segment返回给NetworkBufferPool
					returnMemorySegment(segment);
					return;
				} else {
					// 否则获取BufferListener
					listener = registeredListeners.poll();
					if (listener == null) {
						// 如果listener为空，则将Segment添加到availableMemorySegments集合中
						boolean wasUnavailable = availableMemorySegments.isEmpty();
						availableMemorySegments.add(segment);
						if (wasUnavailable) {
							toNotify = availabilityHelper.getUnavailableToResetAvailable();
						}
						break;
					}
				}
			}
			// 通知listener当前Segment可用，并返回通知结果
			notificationResult = fireBufferAvailableNotification(listener, segment);
		}
		// 通知LocalBufferPool可用
		mayNotifyAvailable(toNotify);
	}

	private NotificationResult fireBufferAvailableNotification(BufferListener listener, MemorySegment segment) {
		// We do not know which locks have been acquired before the recycle() or are needed in the
		// notification and which other threads also access them.
		// -> call notifyBufferAvailable() outside of the synchronized block to avoid a deadlock (FLINK-9676)
		NotificationResult notificationResult = listener.notifyBufferAvailable(new NetworkBuffer(segment, this));
		if (notificationResult.needsMoreBuffers()) {
			synchronized (availableMemorySegments) {
				if (isDestroyed) {
					// cleanup tasks how they would have been done if we only had one synchronized block
					listener.notifyBufferDestroyed();
				} else {
					registeredListeners.add(listener);
				}
			}
		}
		return notificationResult;
	}

	/**
	 * Destroy is called after the produce or consume phase of a task finishes.
	 */
	@Override
	public void lazyDestroy() {
		// NOTE: if you change this logic, be sure to update recycle() as well!
		CompletableFuture<?> toNotify = null;
		synchronized (availableMemorySegments) {
			if (!isDestroyed) {
				MemorySegment segment;
				while ((segment = availableMemorySegments.poll()) != null) {
					returnMemorySegment(segment);
				}

				BufferListener listener;
				while ((listener = registeredListeners.poll()) != null) {
					listener.notifyBufferDestroyed();
				}

				if (!isAvailable()) {
					toNotify = availabilityHelper.getAvailableFuture();
				}

				isDestroyed = true;
			}
		}

		mayNotifyAvailable(toNotify);

		try {
			networkBufferPool.destroyBufferPool(this);
		} catch (IOException e) {
			ExceptionUtils.rethrow(e);
		}
	}

	@Override
	public boolean addBufferListener(BufferListener listener) {
		synchronized (availableMemorySegments) {
			if (!availableMemorySegments.isEmpty() || isDestroyed) {
				return false;
			}

			registeredListeners.add(listener);
			return true;
		}
	}

	@Override
	public void setNumBuffers(int numBuffers) throws IOException {
		int numExcessBuffers;
		CompletableFuture<?> toNotify = null;
		synchronized (availableMemorySegments) {
			checkArgument(numBuffers >= numberOfRequiredMemorySegments,
					"Buffer pool needs at least %s buffers, but tried to set to %s",
					numberOfRequiredMemorySegments, numBuffers);

			if (numBuffers > maxNumberOfMemorySegments) {
				currentPoolSize = maxNumberOfMemorySegments;
			} else {
				currentPoolSize = numBuffers;
			}

			returnExcessMemorySegments();

			numExcessBuffers = numberOfRequestedMemorySegments - currentPoolSize;
			if (numExcessBuffers < 0 && availableMemorySegments.isEmpty() && networkBufferPool.isAvailable()) {
				toNotify = availabilityHelper.getUnavailableToResetUnavailable();
			}
		}

		mayNotifyAvailable(toNotify);

		// If there is a registered owner and we have still requested more buffers than our
		// size, trigger a recycle via the owner.
		if (bufferPoolOwner != null && numExcessBuffers > 0) {
			bufferPoolOwner.releaseMemory(numExcessBuffers);
		}
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (numberOfRequestedMemorySegments >= currentPoolSize) {
			return availabilityHelper.getAvailableFuture();
		} else if (availabilityHelper.isApproximatelyAvailable() || networkBufferPool.isApproximatelyAvailable()) {
			return AVAILABLE;
		} else {
			return CompletableFuture.anyOf(availabilityHelper.getAvailableFuture(), networkBufferPool.getAvailableFuture());
		}
	}

	@Override
	public String toString() {
		synchronized (availableMemorySegments) {
			return String.format(
				"[size: %d, required: %d, requested: %d, available: %d, max: %d, listeners: %d, destroyed: %s]",
				currentPoolSize, numberOfRequiredMemorySegments, numberOfRequestedMemorySegments,
				availableMemorySegments.size(), maxNumberOfMemorySegments, registeredListeners.size(), isDestroyed);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Notifies the potential segment consumer of the new available segments by
	 * completing the previous uncompleted future.
	 */
	private void mayNotifyAvailable(@Nullable CompletableFuture<?> toNotify) {
		if (toNotify != null) {
			toNotify.complete(null);
		}
	}

	private void returnMemorySegment(MemorySegment segment) {
		assert Thread.holdsLock(availableMemorySegments);

		numberOfRequestedMemorySegments--;
		networkBufferPool.recycle(segment);
	}

	private void returnExcessMemorySegments() {
		assert Thread.holdsLock(availableMemorySegments);

		while (numberOfRequestedMemorySegments > currentPoolSize) {
			MemorySegment segment = availableMemorySegments.poll();
			if (segment == null) {
				return;
			}

			returnMemorySegment(segment);
		}
	}
}
