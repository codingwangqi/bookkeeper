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

package org.apache.distributedlog.namespace.etcd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdTestBase;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link EtcdLogMetadataStore}.
 */
@Slf4j
public class EtcdLogMetadataStoreTest extends EtcdTestBase {

    private URI uri;
    private EtcdLogMetadataStore metadataStore;
    private ScheduledExecutorService scheduler;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.uri = URI.create(
            etcdContainer.getClientEndpoint() + "/namespace-" + RandomStringUtils.randomAlphabetic(8)
        );
        this.metadataStore = new EtcdLogMetadataStore(
            etcdClient,
            uri,
            scheduler
        );
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (null != scheduler) {
            scheduler.shutdown();
        }
        super.tearDown();
    }

    @Test
    public void testGetLogLocation() throws Exception {
        assertEquals(
            uri,
            metadataStore.getLogLocation("/path/to/any/log").join().get());
    }

    @Test
    public void testCreateLog() throws Exception {
        String logName = "log-" + RandomStringUtils.randomAlphabetic(16);
        URI createdUri = metadataStore.createLog(logName).join();
        assertEquals(EtcdUtils.getLogUri(uri, logName), createdUri);
        log.info("Successfully created log {}", createdUri);

        // check the log is created
        Iterator<String> logIter = metadataStore.getLogs("").get();
        assertTrue(logIter.hasNext());
        assertEquals(logName, logIter.next());
    }


}
