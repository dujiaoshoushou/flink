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

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.akka.ActorSystemScheduledExecutorAdapter;
import org.apache.flink.runtime.rpc.FencedMainThreadExecutable;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;

import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Identify;
import akka.actor.Props;
import akka.dispatch.Futures;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.concurrent.Future;
import scala.reflect.ClassTag$;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Akka based {@link RpcService} implementation. The RPC service starts an Akka actor to receive
 * RPC invocations from a {@link RpcGateway}.
 */
@ThreadSafe
public class AkkaRpcService implements RpcService {

	private static final Logger LOG = LoggerFactory.getLogger(AkkaRpcService.class);

	static final int VERSION = 1;

	private final Object lock = new Object();

	private final ActorSystem actorSystem;
	private final AkkaRpcServiceConfiguration configuration;

	@GuardedBy("lock")
	private final Map<ActorRef, RpcEndpoint> actors = new HashMap<>(4);

	private final String address;
	private final int port;

	private final ScheduledExecutor internalScheduledExecutor;

	private final CompletableFuture<Void> terminationFuture;

	private volatile boolean stopped;

	public AkkaRpcService(final ActorSystem actorSystem, final AkkaRpcServiceConfiguration configuration) {
		this.actorSystem = checkNotNull(actorSystem, "actor system");
		this.configuration = checkNotNull(configuration, "akka rpc service configuration");

		Address actorSystemAddress = AkkaUtils.getAddress(actorSystem);

		if (actorSystemAddress.host().isDefined()) {
			address = actorSystemAddress.host().get();
		} else {
			address = "";
		}

		if (actorSystemAddress.port().isDefined()) {
			port = (Integer) actorSystemAddress.port().get();
		} else {
			port = -1;
		}

		internalScheduledExecutor = new ActorSystemScheduledExecutorAdapter(actorSystem);

		terminationFuture = new CompletableFuture<>();

		stopped = false;
	}

	public ActorSystem getActorSystem() {
		return actorSystem;
	}

	protected int getVersion() {
		return VERSION;
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public int getPort() {
		return port;
	}

	// this method does not mutate state and is thus thread-safe
	@Override
	public <C extends RpcGateway> CompletableFuture<C> connect(
			final String address,
			final Class<C> clazz) {

		return connectInternal(
			address,
			clazz,
			(ActorRef actorRef) -> {
				Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

				return new AkkaInvocationHandler(
					addressHostname.f0,
					addressHostname.f1,
					actorRef,
					configuration.getTimeout(),
					configuration.getMaximumFramesize(),
					null);
			});
	}

	// this method does not mutate state and is thus thread-safe
	@Override
	public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(String address, F fencingToken, Class<C> clazz) {
		return connectInternal(
			address,
			clazz,
			(ActorRef actorRef) -> {
				Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);

				return new FencedAkkaInvocationHandler<>(
					addressHostname.f0,
					addressHostname.f1,
					actorRef,
					configuration.getTimeout(),
					configuration.getMaximumFramesize(),
					null,
					() -> fencingToken);
			});
	}

	/**
	 * 1. 根据RpcEndpoint是否为FencedRpcEndpoint创建akkaRpcActorProps对象，用于通过actorSystem创建相应的Actor的ActorRef引用类。
	 *    例如：FencedRpcEndpoint会使用FencedAkkaRpcActor创建akkaRpcActorProps配置类。
	 * 2. 根据akkaRpcActorProps的配置信息创建ActorRef实例，这里调用了 actorSystem.actorOf(akkaRpcActorProps, rpcEndpoint.getEndpointId())方法
	 *    创建指定akkaRpcActor的ActotRef对象，创建完毕后会将RpcEndpoint和ActorRef信息存储在Actor键值对集合中。
	 * 3. 启动RpcEndpoint对应的RPC服务，首先获取当前RpcEndpoint实现的RpcGateways接口。其中包括默认的RpcGateway接口，如果RpcServer、AkkaBasedEndpoint，
	 *    还有RpcEndpoint各个实现类自身的RpcGateway接口。RpcGateway接口最终通过RpcUtils.extractImplementedRpcGateways()方法从类定义抽取出来，例如
	 *    JobMaster组件会抽取JobMasterGateway接口定义。
	 * 4. 创建InvocationHandler代理类，事先定义动态代理类InvocationHandler，根据InvocationHandler代理类提供的invoke()方法实现被代理类的具体方法，处理
	 *    本地Runnable线程和远程由Akka系统创建的RpcInvocationMessage消息类对应的方法。
	 * 5. 根据RpcEndpoint是否为FencedRpcEndpoint，InvocationHandler分为FencedAkkaInvocationHandler和AkkaInvocationHandler两种类型。
	 *    FencedAkkaInvocationHandler代理的接口主要有FencedMainThreadExecutable, FencedRpcGateway两种。
	 *    AkkaInvocationHandler主要代理实现InvocationHandler, AkkaBasedEndpoint, RpcServer, StartStoppable, MainThreadExecutable, RpcGateway等接口。
	 * 6. 创建好InvocationHandler代理类后，将当前类的ClassLoader、InvocationHandler实例以及implementedRpcGateways等参数传递到Proxy.newProxyInstance()方法
	 *    中，通过反射的反射创建代理类。创建的代理类会被转换为RpcServer实例，在返回给RpcEndpoint使用。
	 */
	@Override
	public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
		checkNotNull(rpcEndpoint, "rpc endpoint");

		CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
		// 根据RpcEndpoint类型创建不同类型的Prop
		final Props akkaRpcActorProps;

		if (rpcEndpoint instanceof FencedRpcEndpoint) {
			akkaRpcActorProps = Props.create(
				FencedAkkaRpcActor.class,
				rpcEndpoint,
				terminationFuture,
				getVersion(),
				configuration.getMaximumFramesize());
		} else {
			akkaRpcActorProps = Props.create(
				AkkaRpcActor.class,
				rpcEndpoint,
				terminationFuture,
				getVersion(),
				configuration.getMaximumFramesize());
		}
		// 同步块，创建Actor并获取对应的ActorRef
		ActorRef actorRef;

		synchronized (lock) {
			checkState(!stopped, "RpcService is stopped");
			actorRef = actorSystem.actorOf(akkaRpcActorProps, rpcEndpoint.getEndpointId());
			actors.put(actorRef, rpcEndpoint);
		}
        // 启动RpcEndpoint对应的RPC服务
		LOG.info("Starting RPC endpoint for {} at {} .", rpcEndpoint.getClass().getName(), actorRef.path());
		// 获取Actor的路径
		final String akkaAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
		final String hostname;
		Option<String> host = actorRef.path().address().host();
		if (host.isEmpty()) {
			hostname = "localhost";
		} else {
			hostname = host.get();
		}
		// 解析RpcEndpoint实现的所有RpcGateway接口
		Set<Class<?>> implementedRpcGateways = new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass()));
		// 额外添加RpcServer和AkkaBasedEndpoint类
		implementedRpcGateways.add(RpcServer.class);
		implementedRpcGateways.add(AkkaBasedEndpoint.class);
		// 根据不同的RpcEndpoint类型动态创建代理对象
		final InvocationHandler akkaInvocationHandler;

		if (rpcEndpoint instanceof FencedRpcEndpoint) {
			// a FencedRpcEndpoint needs a FencedAkkaInvocationHandler
			akkaInvocationHandler = new FencedAkkaInvocationHandler<>(
				akkaAddress,
				hostname,
				actorRef,
				configuration.getTimeout(),
				configuration.getMaximumFramesize(),
				terminationFuture,
				((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken);

			implementedRpcGateways.add(FencedMainThreadExecutable.class);
		} else {
			akkaInvocationHandler = new AkkaInvocationHandler(
				akkaAddress,
				hostname,
				actorRef,
				configuration.getTimeout(),
				configuration.getMaximumFramesize(),
				terminationFuture);
		}

		// Rather than using the System ClassLoader directly, we derive the ClassLoader
		// from this class . That works better in cases where Flink runs embedded and all Flink
		// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
		// 生成RpcServer对象，然后对该服务的调用都会进入Handler的invoke()方法中处理，Handler实现了多个接口的方法。
		ClassLoader classLoader = getClass().getClassLoader();

		@SuppressWarnings("unchecked")
		RpcServer server = (RpcServer) Proxy.newProxyInstance(
			classLoader,
			implementedRpcGateways.toArray(new Class<?>[implementedRpcGateways.size()]),
			akkaInvocationHandler);

		return server;
	}

	@Override
	public <F extends Serializable> RpcServer fenceRpcServer(RpcServer rpcServer, F fencingToken) {
		if (rpcServer instanceof AkkaBasedEndpoint) {

			InvocationHandler fencedInvocationHandler = new FencedAkkaInvocationHandler<>(
				rpcServer.getAddress(),
				rpcServer.getHostname(),
				((AkkaBasedEndpoint) rpcServer).getActorRef(),
				configuration.getTimeout(),
				configuration.getMaximumFramesize(),
				null,
				() -> fencingToken);

			// Rather than using the System ClassLoader directly, we derive the ClassLoader
			// from this class . That works better in cases where Flink runs embedded and all Flink
			// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
			ClassLoader classLoader = getClass().getClassLoader();

			return (RpcServer) Proxy.newProxyInstance(
				classLoader,
				new Class<?>[]{RpcServer.class, AkkaBasedEndpoint.class},
				fencedInvocationHandler);
		} else {
			throw new RuntimeException("The given RpcServer must implement the AkkaGateway in order to fence it.");
		}
	}

	@Override
	public void stopServer(RpcServer selfGateway) {
		if (selfGateway instanceof AkkaBasedEndpoint) {
			final AkkaBasedEndpoint akkaClient = (AkkaBasedEndpoint) selfGateway;
			final RpcEndpoint rpcEndpoint;

			synchronized (lock) {
				if (stopped) {
					return;
				} else {
					rpcEndpoint = actors.remove(akkaClient.getActorRef());
				}
			}

			if (rpcEndpoint != null) {
				terminateAkkaRpcActor(akkaClient.getActorRef(), rpcEndpoint);
			} else {
				LOG.debug("RPC endpoint {} already stopped or from different RPC service", selfGateway.getAddress());
			}
		}
	}

	@Override
	public CompletableFuture<Void> stopService() {
		final CompletableFuture<Void> akkaRpcActorsTerminationFuture;

		synchronized (lock) {
			if (stopped) {
				return terminationFuture;
			}

			LOG.info("Stopping Akka RPC service.");

			stopped = true;

			akkaRpcActorsTerminationFuture = terminateAkkaRpcActors();
		}

		final CompletableFuture<Void> actorSystemTerminationFuture = FutureUtils.composeAfterwards(
			akkaRpcActorsTerminationFuture,
			() -> FutureUtils.toJava(actorSystem.terminate()));

		actorSystemTerminationFuture.whenComplete(
			(Void ignored, Throwable throwable) -> {
				if (throwable != null) {
					terminationFuture.completeExceptionally(throwable);
				} else {
					terminationFuture.complete(null);
				}

				LOG.info("Stopped Akka RPC service.");
			});

		return terminationFuture;
	}

	@GuardedBy("lock")
	@Nonnull
	private CompletableFuture<Void> terminateAkkaRpcActors() {
		final Collection<CompletableFuture<Void>> akkaRpcActorTerminationFutures = new ArrayList<>(actors.size());

		for (Map.Entry<ActorRef, RpcEndpoint> actorRefRpcEndpointEntry : actors.entrySet()) {
			akkaRpcActorTerminationFutures.add(terminateAkkaRpcActor(actorRefRpcEndpointEntry.getKey(), actorRefRpcEndpointEntry.getValue()));
		}
		actors.clear();

		return FutureUtils.waitForAll(akkaRpcActorTerminationFutures);
	}

	private CompletableFuture<Void> terminateAkkaRpcActor(ActorRef akkaRpcActorRef, RpcEndpoint rpcEndpoint) {
		akkaRpcActorRef.tell(ControlMessages.TERMINATE, ActorRef.noSender());

		return rpcEndpoint.getTerminationFuture();
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	@Override
	public Executor getExecutor() {
		return actorSystem.dispatcher();
	}

	@Override
	public ScheduledExecutor getScheduledExecutor() {
		return internalScheduledExecutor;
	}

	@Override
	public ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
		checkNotNull(runnable, "runnable");
		checkNotNull(unit, "unit");
		checkArgument(delay >= 0L, "delay must be zero or larger");

		return internalScheduledExecutor.schedule(runnable, delay, unit);
	}

	@Override
	public void execute(Runnable runnable) {
		actorSystem.dispatcher().execute(runnable);
	}

	@Override
	public <T> CompletableFuture<T> execute(Callable<T> callable) {
		Future<T> scalaFuture = Futures.<T>future(callable, actorSystem.dispatcher());

		return FutureUtils.toJava(scalaFuture);
	}

	// ---------------------------------------------------------------------------------------
	// Private helper methods
	// ---------------------------------------------------------------------------------------

	private Tuple2<String, String> extractAddressHostname(ActorRef actorRef) {
		final String actorAddress = AkkaUtils.getAkkaURL(actorSystem, actorRef);
		final String hostname;
		Option<String> host = actorRef.path().address().host();
		if (host.isEmpty()) {
			hostname = "localhost";
		} else {
			hostname = host.get();
		}

		return Tuple2.of(actorAddress, hostname);
	}

	/**
	 * 内部连接rpc各个组件方法
	 * 1. 根据指定的Address调用actorSystem.actorSelection(address)方法创建ActorSelection实例，使用ActorSelection对象向该路径指向Actor对象发送消息。
	 * 2. 调用Patterns.ask()方法，向ActorSelection指定的路径发送Identity消息。
	 * 3. 调用FutureUtils.toJava()方法，将Scala类型的Future对象转换成java CompletableFuture对象。
	 * 4. 通过idengtifyFuture获取actorRefFuture对象，并从中获取ActorRef引用对象。
	 * 5. 调用Patterns.ask()方法，向actorRef对应的RpcEndpoint节点发送RemoteHandshakeMessage消息，确保连接的RpcEndpoint节点正常，如果成功，则RpcEndpoint
	 *    会返回HandshakeSuccessMessage消息。
	 * 6. 调用invocationHandlerFactory创建invocationHandler动态代理类，此时可以看到传递的接口列表为new Class<?>[]{clazz}，也就是当前RpcEndpoint需要访问的
	 *    RpcGateway接口，例如JobMaster访问RsourceManager时，这里就是ResourceManagerGateway接口。
	 */
	private <C extends RpcGateway> CompletableFuture<C> connectInternal(
			final String address,
			final Class<C> clazz,
			Function<ActorRef, InvocationHandler> invocationHandlerFactory) {
		checkState(!stopped, "RpcService is stopped");

		LOG.debug("Try to connect to remote RPC endpoint with address {}. Returning a {} gateway.",
			address, clazz.getName());
		// 根据Address创建ActorSelection
		final ActorSelection actorSel = actorSystem.actorSelection(address);
		// 调用Patterns.ask()方法创建ActorIdentity对象
		final Future<ActorIdentity> identify = Patterns
			.ask(actorSel, new Identify(42), configuration.getTimeout().toMilliseconds())
			.<ActorIdentity>mapTo(ClassTag$.MODULE$.<ActorIdentity>apply(ActorIdentity.class));
		// 将identify对象转换为Java CompletableFuture类型对象
		final CompletableFuture<ActorIdentity> identifyFuture = FutureUtils.toJava(identify);
		// 从identifyFuture中获取ActorRef实例
		final CompletableFuture<ActorRef> actorRefFuture = identifyFuture.thenApply(
			(ActorIdentity actorIdentity) -> {
				if (actorIdentity.getRef() == null) {
					throw new CompletionException(new RpcConnectionException("Could not connect to rpc endpoint under address " + address + '.'));
				} else {
					return actorIdentity.getRef();
				}
			});
		// 进行handshake操作，确保需要连接的RpcEndpoint节点正常
		final CompletableFuture<HandshakeSuccessMessage> handshakeFuture = actorRefFuture.thenCompose(
			(ActorRef actorRef) -> FutureUtils.toJava(
				Patterns
					.ask(actorRef, new RemoteHandshakeMessage(clazz, getVersion()), configuration.getTimeout().toMilliseconds())
					.<HandshakeSuccessMessage>mapTo(ClassTag$.MODULE$.<HandshakeSuccessMessage>apply(HandshakeSuccessMessage.class))));

		return actorRefFuture.thenCombineAsync(
			handshakeFuture,
			(ActorRef actorRef, HandshakeSuccessMessage ignored) -> {
				InvocationHandler invocationHandler = invocationHandlerFactory.apply(actorRef);

				// Rather than using the System ClassLoader directly, we derive the ClassLoader
				// from this class . That works better in cases where Flink runs embedded and all Flink
				// code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
				ClassLoader classLoader = getClass().getClassLoader();
				// 创建RPC动态代理类
				@SuppressWarnings("unchecked")
				C proxy = (C) Proxy.newProxyInstance(
					classLoader,
					new Class<?>[]{clazz},
					invocationHandler);

				return proxy;
			},
			actorSystem.dispatcher());
	}
}
