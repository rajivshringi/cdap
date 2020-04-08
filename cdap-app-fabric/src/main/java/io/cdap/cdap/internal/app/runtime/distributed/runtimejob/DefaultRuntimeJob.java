/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.app.runtime.distributed.runtimejob;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.guice.DefaultProgramRunnerFactory;
import io.cdap.cdap.app.guice.RemoteExecutionDiscoveryModule;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.common.UncaughtExceptionHandler;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NoLookupNamespacePathLocator;
import io.cdap.cdap.common.security.KeyStores;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.Networks;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.program.MessagingProgramStateWriter;
import io.cdap.cdap.internal.app.runtime.AbstractListener;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.codec.ArgumentsCodec;
import io.cdap.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedMapReduceProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkerProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedWorkflowProgramRunner;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeMonitorServer;
import io.cdap.cdap.internal.app.runtime.monitor.ServiceSocksProxyInfo;
import io.cdap.cdap.internal.app.runtime.monitor.TrafficRelayServer;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.appender.loader.LogAppenderLoaderService;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.guice.TMSLogAppenderModule;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.messaging.server.MessagingHttpService;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJob;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobEnvironment;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.UGIProvider;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Default implementation of a {@link RuntimeJob}. This class is responsible for submitting cdap program to a
 * {@link TwillRunner} provided by {@link RuntimeJobEnvironment}.
 */
public class DefaultRuntimeJob implements RuntimeJob {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultRuntimeJob.class);
  private static final Logger OUTAGE_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(TimeUnit.SECONDS.toMillis(30)));

  private static final Gson GSON =
    ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec()).create();

  private final CompletableFuture<ProgramController> controllerFuture = new CompletableFuture<>();

  @Override
  public void run(RuntimeJobEnvironment runtimeJobEnv) throws Exception {
    // Setup process wide settings
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();

    // Get Program Options
    ProgramOptions programOptions = readJsonFile(new File(DistributedProgramRunner.PROGRAM_OPTIONS_FILE_NAME),
                                                 ProgramOptions.class);
    ProgramRunId programRunId = programOptions.getProgramId().run(ProgramRunners.getRunId(programOptions));
    ProgramId programId = programRunId.getParent();

    // Setup logging context for the program
    Arguments systemArgs = programOptions.getArguments();
    LoggingContextAccessor.setLoggingContext(LoggingContextHelper.getLoggingContextWithRunId(programRunId,
                                                                                             systemArgs.asMap()));


    // Get App spec
    ApplicationSpecification appSpec = readJsonFile(new File(DistributedProgramRunner.APP_SPEC_FILE_NAME),
                                                    ApplicationSpecification.class);
    ProgramDescriptor programDescriptor = new ProgramDescriptor(programId, appSpec);

    // Create injector and get program runner
    Injector injector = Guice.createInjector(createModules(runtimeJobEnv, createCConf(runtimeJobEnv)));

    LogAppenderInitializer logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    Deque<Service> coreServices = createCoreServices(injector);

    ProxySelector oldProxySelector = ProxySelector.getDefault();
    ProxySelector.setDefault(injector.getInstance(ProxySelector.class));
    startCoreServices(coreServices, logAppenderInitializer);

    try {
      SystemArguments.setLogLevel(programOptions.getUserArguments(), logAppenderInitializer);
      CConfiguration cConf = injector.getInstance(CConfiguration.class);
      ProgramRunner programRunner = injector.getInstance(ProgramRunnerFactory.class).create(programId.getType());

      // Create and run the program. The program files should be present in current working directory.
      try (Program program = createProgram(cConf, programRunner, programDescriptor, programOptions)) {
        CompletableFuture<ProgramController.State> programCompletion = new CompletableFuture<>();
        ProgramController controller = programRunner.run(program, programOptions);
        controllerFuture.complete(controller);

        controller.addListener(new AbstractListener() {
          @Override
          public void completed() {
            programCompletion.complete(ProgramController.State.COMPLETED);
          }

          @Override
          public void killed() {
            programCompletion.complete(ProgramController.State.KILLED);
          }

          @Override
          public void error(Throwable cause) {
            programCompletion.completeExceptionally(cause);
          }
        }, Threads.SAME_THREAD_EXECUTOR);

        // Block on the completion
        programCompletion.get();
      } finally {
        if (programRunner instanceof Closeable) {
          Closeables.closeQuietly((Closeable) programRunner);
        }
      }
    } catch (Throwable t) {
      controllerFuture.completeExceptionally(t);
      throw t;
    } finally {
      stopCoreServices(coreServices, logAppenderInitializer);
      ProxySelector.setDefault(oldProxySelector);
    }
  }

  /**
   * Stops the running program before it completes. It is supposed to be called when user request to stop a run.
   */
  private void stop() {
    try {
      ProgramController controller = controllerFuture.getNow(null);
      if (controller == null) {
        throw new IllegalStateException("Program hasn't been started");
      }
      controller.stop();
    } catch (Exception e) {
      LOG.warn("Cannot stop a failed program", e);
    }
  }

  /**
   * Create {@link CConfiguration} with the given {@link RuntimeJobEnvironment}.
   * Properties returned by the {@link RuntimeJobEnvironment#getProperties()} will be set into the returned
   * {@link CConfiguration} instance.
   */
  private CConfiguration createCConf(RuntimeJobEnvironment runtimeJobEnv) throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.clear();
    cConf.addResource(new File(DistributedProgramRunner.CDAP_CONF_FILE_NAME).toURI().toURL());
    for (Map.Entry<String, String> entry : runtimeJobEnv.getProperties().entrySet()) {
      cConf.set(entry.getKey(), entry.getValue());
    }

    String hostName = InetAddress.getLocalHost().getCanonicalHostName();
    cConf.set(Constants.Service.MASTER_SERVICES_BIND_ADDRESS, hostName);

    return cConf;
  }

  private static <T> T readJsonFile(File file, Class<T> type) {
    try (Reader reader = new BufferedReader(new FileReader(file))) {
      return GSON.fromJson(reader, type);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Unable to read %s file at %s", type.getSimpleName(), file.getAbsolutePath()), e);
    }
  }

  private Program createProgram(CConfiguration cConf, ProgramRunner programRunner,
                                ProgramDescriptor programDescriptor, ProgramOptions options) throws IOException {
    File tempDir = createTempDirectory(cConf, options.getProgramId(), options.getArguments()
      .getOption(ProgramOptionConstants.RUN_ID));
    File programDir = new File(tempDir, "program");
    DirUtils.mkdirs(programDir);
    File programJarFile = new File(programDir, "program.jar");
    Location programJarLocation = Locations.toLocation(
      new File(options.getArguments().getOption(ProgramOptionConstants.PROGRAM_JAR)));
    Locations.linkOrCopy(programJarLocation, programJarFile);
    programJarLocation = Locations.toLocation(programJarFile);
    BundleJarUtil.unJar(programJarLocation, programDir);

    return Programs.create(cConf, programRunner, programDescriptor, programJarLocation, programDir);
  }

  private File createTempDirectory(CConfiguration cConf, ProgramId programId, String runId) {
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR)).getAbsoluteFile();
    File dir = new File(tempDir, String.format("%s.%s.%s.%s.%s",
                                               programId.getType().name().toLowerCase(),
                                               programId.getNamespace(), programId.getApplication(),
                                               programId.getProgram(), runId));
    DirUtils.mkdirs(dir);
    return dir;
  }

  /**
   * Returns list of guice modules used to start the program run.
   */
  @VisibleForTesting
  List<Module> createModules(RuntimeJobEnvironment runtimeJobEnv, CConfiguration cConf) {
    List<Module> modules = new ArrayList<>();
    modules.add(new ConfigModule(cConf));
    modules.add(new IOModule());
    modules.add(new TMSLogAppenderModule());
    modules.add(new RemoteExecutionDiscoveryModule());
    modules.add(new AuthenticationContextModules().getProgramContainerModule());
    modules.add(new MetricsClientRuntimeModule().getDistributedModules());
    modules.add(new MessagingServerRuntimeModule().getStandaloneModules());

    modules.add(new AbstractModule() {
      @Override
      protected void configure() {
        bind(ClusterMode.class).toInstance(ClusterMode.ISOLATED);
        bind(UGIProvider.class).to(CurrentUGIProvider.class).in(Scopes.SINGLETON);

        // In isolated mode, ignore the namespace mapping
        bind(NamespacePathLocator.class).to(NoLookupNamespacePathLocator.class);

        // Bindings from the environment
        bind(TwillRunner.class).annotatedWith(Constants.AppFabric.ProgramRunner.class)
          .toInstance(runtimeJobEnv.getTwillRunner());
        bind(LocationFactory.class).toInstance(runtimeJobEnv.getLocationFactory());

        MapBinder<ProgramType, ProgramRunner> defaultProgramRunnerBinder = MapBinder.newMapBinder(
          binder(), ProgramType.class, ProgramRunner.class);

        bind(ProgramRuntimeProvider.Mode.class).toInstance(ProgramRuntimeProvider.Mode.DISTRIBUTED);
        bind(ProgramRunnerFactory.class).annotatedWith(Constants.AppFabric.ProgramRunner.class)
          .to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
        bind(ProgramStateWriter.class).to(MessagingProgramStateWriter.class).in(Scopes.SINGLETON);

        defaultProgramRunnerBinder.addBinding(ProgramType.MAPREDUCE).to(DistributedMapReduceProgramRunner.class);
        defaultProgramRunnerBinder.addBinding(ProgramType.WORKFLOW).to(DistributedWorkflowProgramRunner.class);
        defaultProgramRunnerBinder.addBinding(ProgramType.WORKER).to(DistributedWorkerProgramRunner.class);
        bind(ProgramRunnerFactory.class).to(DefaultProgramRunnerFactory.class).in(Scopes.SINGLETON);
      }
    });

    // Active monitoring means we need to start the RuntimeMonitorServer for app-fabric to poll
    if (cConf.getBoolean(Constants.RuntimeMonitor.ACTIVE_MONITORING, false)) {
      modules.add(createRuntimeMonitorServerModule(cConf));
    }

    return modules;
  }

  /**
   * Optionally adds {@link RuntimeMonitorServer} binding.
   */
  private Module createRuntimeMonitorServerModule(CConfiguration cConf) {
    return new PrivateModule() {
      @Override
      protected void configure() {
        try {
          Path keyStorePath = Paths.get(cConf.get(Constants.RuntimeMonitor.SERVER_KEYSTORE_PATH));
          Path trustStorePath = Paths.get(cConf.get(Constants.RuntimeMonitor.CLIENT_KEYSTORE_PATH));

          KeyStore keyStore = KeyStores.load(Locations.toLocation(keyStorePath), () -> "");
          KeyStore trustStore = KeyStores.load(Locations.toLocation(trustStorePath), () -> "");

          // Update the cConf as well to store the service proxy password
          cConf.set(Constants.RuntimeMonitor.SERVICE_PROXY_PASSWORD, KeyStores.hash(keyStore));

          bind(KeyStore.class).annotatedWith(Constants.AppFabric.KeyStore.class).toInstance(keyStore);
          bind(KeyStore.class).annotatedWith(Constants.AppFabric.TrustStore.class).toInstance(trustStore);

          bind(Cancellable.class).toInstance(() -> stop());
          bind(RuntimeMonitorServer.class);
          expose(RuntimeMonitorServer.class);

        } catch (Exception e) {
          // Just log if failed to load the KeyStores. It will fail when RuntimeMonitorServer is needed.
          LOG.error("Failed to load key store and/or trust store", e);
        }
      }
    };
  }

  @VisibleForTesting
  Deque<Service> createCoreServices(Injector injector) {
    Deque<Service> services = new LinkedList<>();

    MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);
    services.add(metricsCollectionService);

    services.add(injector.getInstance(LogAppenderLoaderService.class));

    MessagingService messagingService = injector.getInstance(MessagingService.class);
    if (messagingService instanceof Service) {
      services.add((Service) messagingService);
    }
    services.add(injector.getInstance(MessagingHttpService.class));

    // Bind the traffic relay on the host, not on the loopback interface. It needs to be accessible from all workers.
    services.add(injector.getInstance(TrafficRelayService.class));

    CConfiguration cConf = injector.getInstance(CConfiguration.class);
    if (cConf.getBoolean(Constants.RuntimeMonitor.ACTIVE_MONITORING, false)) {
      services.add(injector.getInstance(RuntimeMonitorServer.class));
    }

    return services;
  }

  private void startCoreServices(Deque<Service> coreServices, LogAppenderInitializer logAppenderInitializer) {
    // Initialize log appender
    logAppenderInitializer.initialize();

    try {
      // Starts the core services
      for (Service service : coreServices) {
        LOG.debug("Starting core service {}", service);
        service.startAndWait();
      }
    } catch (Exception e) {
      logAppenderInitializer.close();
      throw e;
    }
  }

  private void stopCoreServices(Deque<Service> coreServices, LogAppenderInitializer logAppenderInitializer) {
    // Stop all services. Reverse the order.
    for (Service service : (Iterable<Service>) coreServices::descendingIterator) {
      LOG.debug("Stopping core service {}", service);
      try {
        service.stopAndWait();
      } catch (Exception e) {
        LOG.warn("Exception raised when stopping service {} during program termination.", service, e);
      }
    }
    logAppenderInitializer.close();
  }

  /**
   * A service wrapper around {@link TrafficRelayServer} for setting address configurations after
   * starting the relay server.
   */
  private static final class TrafficRelayService extends AbstractIdleService {

    private final CConfiguration cConf;
    private TrafficRelayServer relayServer;

    @Inject
    TrafficRelayService(CConfiguration cConf) {
      this.cConf = cConf;
    }

    @Override
    protected void startUp() throws Exception {
      // Bind the traffic relay on the host, not on the loopback interface. It needs to be accessible from all workers.
      relayServer = new TrafficRelayServer(InetAddress.getLocalHost(), this::getTrafficRelayTarget);
      relayServer.startAndWait();

      // Set the traffic relay service address to cConf. It will be used as the proxy address for all worker processes
      Networks.setAddress(cConf, Constants.RuntimeMonitor.SERVICE_PROXY_ADDRESS,
                          ResolvingDiscoverable.resolve(relayServer.getBindAddress()));

      LOG.info("Runtime traffic relay server started on {}", relayServer.getBindAddress());
    }

    @Override
    protected void shutDown() {
      relayServer.stopAndWait();
    }

    @Nullable
    private InetSocketAddress getTrafficRelayTarget() {
      try (Reader reader = Files.newBufferedReader(Paths.get(Constants.RuntimeMonitor.SERVICE_PROXY_FILE),
                                                   StandardCharsets.UTF_8)) {
        int port = GSON.fromJson(reader, ServiceSocksProxyInfo.class).getPort();
        return port == 0 ? null : new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
      } catch (Exception e) {
        OUTAGE_LOG.warn("Failed to open service proxy file {}", Constants.RuntimeMonitor.SERVICE_PROXY_FILE, e);
        return null;
      }
    }
  }
}
