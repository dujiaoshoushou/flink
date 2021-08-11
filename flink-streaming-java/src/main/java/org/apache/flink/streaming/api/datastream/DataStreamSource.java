/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

/**
 * The DataStreamSource represents the starting point of a DataStream.
 *
 * @param <T> Type of the elements in the DataStream created from the this source.
 * DataStreamSource是DataStream的起点，DataStreamSource在 {@link StreamExecutionEnvironment}
 *           中创建，由StreamExecutionEnvironment.addSouce(SourceFunction)创建而来，其中
 *           SourceFunction中包含了DataStreamSource从数据源读取数据的具体逻辑。
 */
@Public
public class DataStreamSource<T> extends SingleOutputStreamOperator<T> {

	boolean isParallel;

	public DataStreamSource(StreamExecutionEnvironment environment,
			TypeInformation<T> outTypeInfo, StreamSource<T, ?> operator,
			boolean isParallel, String sourceName) {
		super(environment, new SourceTransformation<>(sourceName, operator, outTypeInfo, environment.getParallelism()));

		this.isParallel = isParallel;
		if (!isParallel) {
			setParallelism(1);
		}
	}

	public DataStreamSource(SingleOutputStreamOperator<T> operator) {
		super(operator.environment, operator.getTransformation());
		this.isParallel = true;
	}

	@Override
	public DataStreamSource<T> setParallelism(int parallelism) {
		OperatorValidationUtils.validateMaxParallelism(parallelism, isParallel);
		super.setParallelism(parallelism);
		return this;
	}
}
