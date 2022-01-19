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

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * A {@link Trigger} that fires once the watermark passes the end of the window
 * to which a pane belongs.
 *
 * @see org.apache.flink.streaming.api.watermark.Watermark
 */
@PublicEvolving
public class EventTimeTrigger extends Trigger<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private EventTimeTrigger() {}

	/**
	 * 1. 当前数据元素接入后，根据窗口中maxTimestamp是否大于当前算子中的Watermark觉得是否触发窗口计算。
	 * 如果符合触发条件，则返回TriggerResult.FIRE事件，这里的maxTimestamp实际上是窗口的结束时间减1，属于该窗口的最大时间戳。
	 * 2. 如果不满足以上条件，就会继续向TriggerContext中注册Timer定时器，等待指定时间再通过定时器触发窗口计算，
	 * 此时方法会返回TriggerResult.CONTINUE消息给WindowOperator，表示此时窗口不会触发计算，继续等待新的数据接入。
	 * 3. 当数据元素不断接入WindowOperator，不断更新Watermark时，只要Watermark大于窗口的右边界就会触发相应的窗口计算。
	 */
	@Override
	public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
		if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
			// if the watermark is already past the window fire immediately
			// 如果Watermark超过窗口最大时间戳，则立即执行。
			return TriggerResult.FIRE;
		} else {
			ctx.registerEventTimeTimer(window.maxTimestamp());
			return TriggerResult.CONTINUE;
		}
	}

	@Override
	public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
		return time == window.maxTimestamp() ?
			TriggerResult.FIRE :
			TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
		return TriggerResult.CONTINUE;
	}

	@Override
	public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
		ctx.deleteEventTimeTimer(window.maxTimestamp());
	}

	@Override
	public boolean canMerge() {
		return true;
	}

	@Override
	public void onMerge(TimeWindow window,
			OnMergeContext ctx) {
		// only register a timer if the watermark is not yet past the end of the merged window
		// this is in line with the logic in onElement(). If the watermark is past the end of
		// the window onElement() will fire and setting a timer here would fire the window twice.
		long windowMaxTimestamp = window.maxTimestamp();
		if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
			ctx.registerEventTimeTimer(windowMaxTimestamp);
		}
	}

	@Override
	public String toString() {
		return "EventTimeTrigger()";
	}

	/**
	 * Creates an event-time trigger that fires once the watermark passes the end of the window.
	 *
	 * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
	 * trigger window evaluation with just this one element.
	 */
	public static EventTimeTrigger create() {
		return new EventTimeTrigger();
	}
}
