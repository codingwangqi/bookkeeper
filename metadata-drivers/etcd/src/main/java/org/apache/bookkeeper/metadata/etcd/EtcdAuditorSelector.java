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

package org.apache.bookkeeper.metadata.etcd;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.Txn;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.meta.AuditorSelector;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * Etcd based {@link AuditorSelector} implementation.
 */
@Slf4j
class EtcdAuditorSelector implements AuditorSelector {

    private final String scope;
    private final Client client;
    private final boolean ownClient;
    private final KV kvClient;
    private final EtcdBookieRegister bkRegister;
    private final String bookieId;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService executor;
    private final String electionPath;

    // election related variables
    private String myVote;

    @VisibleForTesting
    EtcdAuditorSelector(String bookieId,
                        Client client,
                        String scope) {
        this(bookieId, client, scope, 60);
    }

    EtcdAuditorSelector(String bookieId,
                        Client client,
                        String scope,
                        long ttlSeconds) {
        this(
            bookieId,
            client,
            scope,
            new EtcdBookieRegister(
                client.getLeaseClient(),
                ttlSeconds,
                () -> {}
            ).start(),
            true);
    }

    EtcdAuditorSelector(String bookieId,
                        Client client,
                        String scope,
                        EtcdBookieRegister bkRegister) {
        this(bookieId, client, scope, bkRegister, false);
    }

    EtcdAuditorSelector(String bookieId,
                        Client client,
                        String scope,
                        EtcdBookieRegister bkRegister,
                        boolean ownClient) {
        this.bookieId = bookieId;
        this.scope = scope;
        this.client = client;
        this.kvClient = client.getKVClient();
        this.bkRegister = bkRegister;
        this.ownClient = ownClient;
        this.electionPath = EtcdUtils.getAuditorElectionPath(scope);
        this.executor = Executors.newSingleThreadExecutor(r ->
            new Thread(r, "AuditorElector"));
    }

    private void createMyVote() throws Exception {
        if (null != myVote) {
            String myVotePath = EtcdUtils.getAuditorVotePath(scope, myVote);
            GetResponse getResponse = result(kvClient.get(ByteSequence.fromString(myVotePath)));
            if (getResponse.getCount() >= 1) {
                // key is found, so we are good.
                return;
            }
        }
        myVote = createNewVote();
    }

    private String createNewVote() throws Exception {
        GetResponse getResp = result(kvClient.get(ByteSequence.fromString(electionPath)));
        if (getResp.getCount() != 1) {
            throw new IOException("Failed get value of auditor election path '" + electionPath + "'");
        }
        KeyValue kv = getResp.getKvs().get(0);
        

    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public Future<?> select(SelectorListener listener) throws MetadataException {
        return null;
    }

    @Override
    public BookieSocketAddress getCurrentAuditor() throws MetadataException {
        return null;
    }

    @Override
    public void close() {
        if (ownClient) {
            log.info("Closing auditor selector under scope '{}'", scope);
            bkRegister.close();
            client.close();
            log.info("Successfully closed auditor selector under scope '{}'", scope);
        }
    }
}
