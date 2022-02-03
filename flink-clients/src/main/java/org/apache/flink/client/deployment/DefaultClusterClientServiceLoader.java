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

package org.apache.flink.client.deployment;

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A service provider for {@link ClusterClientFactory cluster client factories}.
 */
public class DefaultClusterClientServiceLoader implements ClusterClientServiceLoader {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultClusterClientServiceLoader.class);
	// 通过Java Service Loaer加载配置的ClusterClientFactory对象
	private static final ServiceLoader<ClusterClientFactory> defaultLoader = ServiceLoader.load(ClusterClientFactory.class);

	@Override
	public <ClusterID> ClusterClientFactory<ClusterID> getClusterClientFactory(final Configuration configuration) {
		checkNotNull(configuration);
        // 创建ClusterClientFactory集合
		final List<ClusterClientFactory> compatibleFactories = new ArrayList<>();
		final Iterator<ClusterClientFactory> factories = defaultLoader.iterator();
		while (factories.hasNext()) {
			try {
				final ClusterClientFactory factory = factories.next();
				// 将符合条件的ClusterClientFactory放入到集合中
				if (factory != null && factory.isCompatibleWith(configuration)) {
					compatibleFactories.add(factory);
				}
			} catch (Throwable e) {
				if (e.getCause() instanceof NoClassDefFoundError) {
					LOG.info("Could not load factory due to missing dependencies.");
				} else {
					throw e;
				}
			}
		}
		// 仅获取一个符合条件的ClusterClientFactory，如果获取多个则抛出异常
		if (compatibleFactories.size() > 1) {
			final List<String> configStr =
					configuration.toMap().entrySet().stream()
							.map(e -> e.getKey() + "=" + e.getValue())
							.collect(Collectors.toList());

			throw new IllegalStateException("Multiple compatible client factories found for:\n" + String.join("\n", configStr) + ".");
		}
		// 返回通过ServiceLoader加载进来ClusterClientFactory
		return compatibleFactories.isEmpty() ? null : (ClusterClientFactory<ClusterID>) compatibleFactories.get(0);
	}
}
