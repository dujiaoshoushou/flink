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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;

/**
 * Base interface for clean up state, both for {@link ProcessFunction} and {@link CoProcessFunction}.
 */
public interface CleanupState {

	default void registerProcessingCleanupTimer(
			ValueState<Long> cleanupTimeState,
			long currentTime,
			long minRetentionTime,
			long maxRetentionTime,
			TimerService timerService) throws Exception {

		// last registered timer 通过cleanupTimeState状态获取最新一次清理状态的注册事件curCleanupTime
		Long curCleanupTime = cleanupTimeState.value();

		// check if a cleanup timer is registered and
		// that the current cleanup timer won't delete state we need to keep
		// 判断当前curCleanupTime是否为空，且currentTime + minRetentionTime总和是否大于curCleanupTime。
		// 只有满足以上两个条件才会触发注册状态数据清理的定时器，这里的minRetentionTime事用户指定的状态保留最短时间。
		if (curCleanupTime == null || (currentTime + minRetentionTime) > curCleanupTime) {
			// we need to register a new (later) timer
			long cleanupTime = currentTime + maxRetentionTime;
			// register timer and remember clean-up time 调用TimeService注册ProcessingTimeTimer，在满足定时器的实际条件后触发定时器。
			timerService.registerProcessingTimeTimer(cleanupTime);
			// delete expired timer
			// 如果curCleanupTime不为空，即之前的TimeService还包含过去的定时器，则调用timerService.deleteProcessingTimeTimer()方法
			// 删除过期的定时器。
			if (curCleanupTime != null) {
				timerService.deleteProcessingTimeTimer(curCleanupTime);
			}
			cleanupTimeState.update(cleanupTime); // 更新cleanupTimeState中的curCleanupTime指标。
		}
	}
}
