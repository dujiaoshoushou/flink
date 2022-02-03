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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.SessionClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.security.SecurityContext;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.util.Map;

/**
 * Entry point for Yarn session clusters.
 */
public class YarnSessionClusterEntrypoint extends SessionClusterEntrypoint {

	public YarnSessionClusterEntrypoint(
		Configuration configuration) {
		super(configuration);
	}

	@Override
	protected SecurityContext installSecurityContext(Configuration configuration) throws Exception {
		return YarnEntrypointUtils.installSecurityContext(configuration);
	}

	@Override
	protected String getRPCPortRange(Configuration configuration) {
		return configuration.getString(YarnConfigOptions.APPLICATION_MASTER_PORT);
	}

	@Override
	protected DispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) {
		return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(YarnResourceManagerFactory.getInstance());
	}

	public static void main(String[] args) {
		// startup checks and logging
		// 启动前检查并设定参数
		EnvironmentInformation.logEnvironmentInfo(LOG, YarnSessionClusterEntrypoint.class.getSimpleName(), args);
		SignalHandler.register(LOG);
		// 安装safeguard shutdown hook
		JvmShutdownSafeguard.installAsShutdownHook(LOG);
		// 获取系统环境变量
		Map<String, String> env = System.getenv();
		// 获取工作路径
		final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
		Preconditions.checkArgument(
			workingDirectory != null,
			"Working directory variable (%s) not set",
			ApplicationConstants.Environment.PWD.key());

		try {
			// 加载Yarn需要的环境信息
			YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
		} catch (IOException e) {
			LOG.warn("Could not log YARN environment information.", e);
		}
		// 创建Configuration配置
		Configuration configuration = YarnEntrypointUtils.loadConfiguration(workingDirectory, env);
		// 创建YarnSessionClusterEntrypoint对象
		YarnSessionClusterEntrypoint yarnSessionClusterEntrypoint = new YarnSessionClusterEntrypoint(configuration);
		// 通过ClusterEntrypoint执行ClusterEntrypoint
		ClusterEntrypoint.runClusterEntrypoint(yarnSessionClusterEntrypoint);
	}
}
