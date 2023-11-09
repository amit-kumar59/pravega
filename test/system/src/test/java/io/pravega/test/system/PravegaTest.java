/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class PravegaTest extends AbstractReadWriteTest {

    private final static String STREAM_NAME = "testStreamSampleY";
    private final static String STREAM_SCOPE = "testScopeSampleY" + randomAlphanumeric(5);
    private final static String READER_GROUP = "ExampleReaderGroupY";
    private final static int NUM_EVENTS = 100;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(4);
    private final StreamConfiguration config = StreamConfiguration.builder()
                                                                  .scalingPolicy(scalingPolicy)
                                                                  .build();

    /**
     * This is used to setup the various services required by the system test framework.
     */
    @Environment
    public static void initialize() {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    /**
     * Invoke the simpleTest, ensure we are able to produce  events.
     * The test fails incase of exceptions while writing to the stream.
     *
     */
    @Test
    public void simpleTest() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        URI controllerUri = ctlURIs.get(0);

        log.info("Invoking create stream with Controller URI: {}", controllerUri);

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(Utils.buildClientConfig(controllerUri));

        log.info("***PravegaTest@simpleTest before controller connection Factory {}", connectionFactory);

        @Cleanup
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                                                                           .clientConfig(Utils.buildClientConfig(controllerUri))
                                                                           .build(), connectionFactory.getInternalExecutor());

        log.info("***PravegaTest@simpleTest after controller connection Factory {}", connectionFactory);

        ControllerImplConfig config1 = ControllerImplConfig.builder()
                .clientConfig(Utils.buildClientConfig(controllerUri)).build();

        log.info("***PravegaTest@simpleTest controller config :: {}",config1);
        log.info("***PravegaTest@simpleTest before scope creation controller {}", controller);

        assertTrue(controller.createScope(STREAM_SCOPE).join());

        log.info("***PravegaTest@simpleTest Stream_SCOPE{} creation has completed ", STREAM_SCOPE);

        assertTrue(controller.createStream(STREAM_SCOPE, STREAM_NAME, config).join());

        log.info("***PravegaTest@simpleTest STREAM_NAME {} creation has completed ", STREAM_NAME);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(STREAM_SCOPE, Utils.buildClientConfig(controllerUri));
        log.info("Invoking Writer test with Controller URI: {}", controllerUri);

        @Cleanup
        EventStreamWriter<Serializable> writer = clientFactory.createEventWriter(STREAM_NAME,
                                                                                 new JavaSerializer<>(),
                                                                                 EventWriterConfig.builder().build());

        log.info("***PravegaTest@simpleTest writer {} has created ", writer.getConfig().toString());
        for (int i = 0; i < NUM_EVENTS; i++) {
            String event = "Publish " + i + "\n";
            //log.debug("Producing event: {} ", event);
            log.info("***** Producing event: {} ", event);
            // any exceptions while writing the event will fail the test.
            writer.writeEvent("", event);
            writer.flush();
        }

        log.info("***PravegaTest@simpleTest written of event has completed.");

        log.info("Invoking Reader test.");
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(STREAM_SCOPE, Utils.buildClientConfig(controllerUri));

        log.info("***PravegaTest@simpleTest ReaderGroupManager  instance has created");

        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().stream(Stream.of(STREAM_SCOPE, STREAM_NAME)).build());

        log.info("***PravegaTest@simpleTest readerGroup ::{} has created", READER_GROUP);

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(UUID.randomUUID().toString(),
                                                                      READER_GROUP,
                                                                      new JavaSerializer<>(),
                                                                      ReaderConfig.builder().build());

        log.info("***PravegaTest@simpleTest reader ::{} has created", reader.getConfig().toString());
        int readCount = 0;

        EventRead<String> event = null;
        log.info("***PravegaTest@simpleTest reader is starting event reading ");
        do {
            event = reader.readNextEvent(10_000);
            //log.debug("Read event: {}.", event.getEvent());
            log.info("***PravegaTest@simpleTest read event :{}",event.getEvent());
            if (event.getEvent() != null) {
                readCount++;
            }
            // try reading until all the written events are read, else the test will timeout.
        } while ((event.getEvent() != null || event.isCheckpoint()) && readCount < NUM_EVENTS);
        assertEquals("Read count should be equal to write count", NUM_EVENTS, readCount);

        log.info("***PravegaTest@simpleTest reader has finished event reading ");
    }
}
