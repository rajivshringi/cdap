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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.metrics.MetricsReporterHook;
import io.cdap.cdap.gateway.handlers.PingHandler;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * The runtime server for accepting runtime calls from the program runtime.
 */
public class RuntimeServer extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(RuntimeServer.class);

  private final NettyHttpService httpService;
  private final DiscoveryService discoveryService;
  private Cancellable cancelDiscovery;

  @Inject
  RuntimeServer(CConfiguration cConf, RuntimeRequestValidator requestValidator,
                DiscoveryService discoveryService, DiscoveryServiceClient discoveryServiceClient,
                MessagingService messagingService, MetricsCollectionService metricsCollectionService,
                RemoteExecutionLogProcessor logProcessor) {
    this.httpService = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.RUNTIME)
      .setHttpHandlers(new PingHandler(),
                       new RuntimeHandler(cConf, new MultiThreadMessagingContext(messagingService),
                                          logProcessor, requestValidator),
                       new RuntimeServiceRoutingHandler(discoveryServiceClient, requestValidator))
      .setExceptionHandler(new HttpExceptionHandler())
      .setHandlerHooks(Collections.singleton(new MetricsReporterHook(metricsCollectionService,
                                                                     Constants.Service.RUNTIME)))
      .setHost(cConf.get(Constants.RuntimeMonitor.BIND_ADDRESS))
      .setPort(cConf.getInt(Constants.RuntimeMonitor.BIND_PORT))
      .build();
    this.discoveryService = discoveryService;
  }

  @Override
  protected void startUp() throws Exception {
    httpService.start();
    Discoverable discoverable = ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.RUNTIME,
                                                                                      httpService));
    cancelDiscovery = discoveryService.register(discoverable);
    LOG.debug("Runtime server with service name '{}' started on {}:{}", discoverable.getName(),
              discoverable.getSocketAddress().getHostName(), discoverable.getSocketAddress().getPort());
  }

  @Override
  protected void shutDown() throws Exception {
    cancelDiscovery.cancel();
    httpService.stop();
  }
}
