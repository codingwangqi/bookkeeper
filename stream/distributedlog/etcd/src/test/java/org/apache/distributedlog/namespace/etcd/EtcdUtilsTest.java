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

import static org.apache.distributedlog.namespace.etcd.EtcdUtils.getLogUri;
import static org.apache.distributedlog.namespace.etcd.EtcdUtils.normalizeLogName;
import static org.apache.distributedlog.namespace.etcd.EtcdUtils.normalizeScope;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import org.junit.Test;

/**
 * Unit test {@link EtcdUtils}.
 */
public class EtcdUtilsTest {

    @Test
    public void testNormalizeScope() {
        assertEquals("test", normalizeScope("test"));
        assertEquals("test", normalizeScope("test/"));
        assertEquals("test", normalizeScope("test//"));
        assertEquals("test", normalizeScope("test///"));
    }

    @Test
    public void testNormalizeLogName() {
        assertEquals("test", normalizeLogName("test"));
        assertEquals("test", normalizeLogName("/test"));
        assertEquals("test", normalizeLogName("//test"));
        assertEquals("test", normalizeLogName("///test"));
    }

    @Test
    public void testGetLogUri() {
        URI logUri = URI.create("distributedlog://localhost/ns/_logs_/path/to/log");
        URI baseUri = URI.create("distributedlog://localhost/ns");
        URI baseUri2 = URI.create("distributedlog://localhost/ns/");
        URI baseUri3 = URI.create("distributedlog://localhost/ns//");

        assertEquals(logUri, getLogUri(baseUri, "/path/to/log"));
        assertEquals(logUri, getLogUri(baseUri2, "/path/to/log"));
        assertEquals(logUri, getLogUri(baseUri3, "/path/to/log"));
    }

}
