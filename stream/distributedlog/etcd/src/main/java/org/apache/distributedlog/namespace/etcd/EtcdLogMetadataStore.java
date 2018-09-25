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

import static org.apache.distributedlog.namespace.etcd.EtcdUtils.getLogName;
import static org.apache.distributedlog.namespace.etcd.EtcdUtils.getLogPath;
import static org.apache.distributedlog.namespace.etcd.EtcdUtils.getLogPrefixBeginKey;
import static org.apache.distributedlog.namespace.etcd.EtcdUtils.getLogPrefixEndKey;
import static org.apache.distributedlog.namespace.etcd.EtcdUtils.getLogUri;
import static org.apache.distributedlog.namespace.etcd.EtcdUtils.getLogsBeginKey;
import static org.apache.distributedlog.namespace.etcd.EtcdUtils.getLogsEndKey;
import static org.apache.distributedlog.namespace.etcd.EtcdUtils.normalizeScope;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.Cmp.Op;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import com.google.common.base.Optional;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.metadata.etcd.helpers.KeySetReader;
import org.apache.bookkeeper.metadata.etcd.helpers.KeyStream;
import org.apache.bookkeeper.metadata.etcd.helpers.KeyStreamIterator;
import org.apache.distributedlog.callback.NamespaceListener;
import org.apache.distributedlog.exceptions.LogExistsException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.metadata.LogMetadataStore;

/**
 * Etcd based log metadata store.
 */
@Slf4j
class EtcdLogMetadataStore implements LogMetadataStore {

    private final Client client;
    private final KV kvClient;
    private final URI namespace;
    private final String scope;
    private final ScheduledExecutorService scheduler;

    EtcdLogMetadataStore(Client client,
                         URI namespace,
                         ScheduledExecutorService scheduler) {
        this.client = client;
        this.kvClient = client.getKVClient();
        this.namespace = namespace;
        this.scope = normalizeScope(namespace.getPath());
        this.scheduler = scheduler;
    }

    @Override
    public CompletableFuture<URI> createLog(String logName) {
        String logPath = getLogPath(scope, logName);
        ByteSequence logPathBs = ByteSequence.fromString(logPath);
        return kvClient.txn()
            .If(new Cmp(
                logPathBs,
                Op.GREATER,
                CmpTarget.createRevision(0L)
            ))
            .Then(com.coreos.jetcd.op.Op.get(
                logPathBs,
                GetOption.newBuilder()
                    .withCountOnly(true)
                    .build()
            ))
            .Else(com.coreos.jetcd.op.Op.put(
                logPathBs,
                EtcdConstants.EMPTY_BS,
                PutOption.DEFAULT
            ))
            .commit()
            .thenCompose(resp -> {
                if (resp.isSucceeded()) {
                    Throwable cause;
                    GetResponse getResp = resp.getGetResponses().get(0);
                    if (getResp.getCount() <= 0) {
                        // log path doesn't exists but we fail to put the key
                        cause = new UnexpectedException(
                            "Log path '" + logPath + "' doesn't exist but we fail to create the path");
                    } else {
                        cause = new LogExistsException(
                            "Log '" + logName + "' already exists under " + namespace);
                    }
                    return FutureUtils.exception(cause);
                } else {
                    return FutureUtils.value(getLogUri(namespace, logName));
                }
            });
    }

    @Override
    public CompletableFuture<Optional<URI>> getLogLocation(String logName) {
        return FutureUtils.value(Optional.of(namespace));
    }

    @Override
    public CompletableFuture<Iterator<String>> getLogs(String logNamePrefix) {
        String beginKey = getLogPrefixBeginKey(scope, logNamePrefix);
        String endKey = getLogPrefixEndKey(scope, logNamePrefix);
        log.info("Get logs between [{} - {}]", beginKey, endKey);
        KeyStream<String> ks = new KeyStream<>(
            kvClient,
            ByteSequence.fromString(beginKey),
            ByteSequence.fromString(endKey),
            bs -> getLogName(scope, bs.toStringUtf8())
        );
        return FutureUtils.value(new KeyStreamIterator<>(ks));
    }

    @Override
    public void registerNamespaceListener(NamespaceListener listener) {
        KeySetReader<String> ksr = new KeySetReader<>(
            client,
            bs -> getLogName(scope, bs.toStringUtf8()),
            ByteSequence.fromString(getLogsBeginKey(scope)),
            ByteSequence.fromString(getLogsEndKey(scope))
        );
        ksr.readAndWatch(logs -> listener.onStreamsChanged(logs.getValue().iterator()))
            .exceptionally(cause -> {
                log.warn("Failed to register namespace listener, will re-attempt registering" +
                    " the namespace listener in 1 second ...", cause);
                scheduler.schedule(() -> registerNamespaceListener(listener),
                    1, TimeUnit.SECONDS);
                return null;
            });

    }
}
