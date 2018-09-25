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

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.distributedlog.namespace.etcd.EtcdConstants.BEGIN_SEP;
import static org.apache.distributedlog.namespace.etcd.EtcdConstants.END_SEP;
import static org.apache.distributedlog.namespace.etcd.EtcdConstants.LOGS_NODE;

import java.net.URI;
import java.net.URISyntaxException;
import lombok.extern.slf4j.Slf4j;

/**
 * Utils for etcd based metadata store.
 */
@Slf4j
final class EtcdUtils {

    private EtcdUtils() {}

    static String normalizeScope(String scope) {
        if (scope.endsWith("/")) {
            return normalizeScope(scope.substring(0, scope.length() - 1));
        } else {
            return scope;
        }
    }

    static String normalizeLogName(String logName) {
        if (logName.startsWith("/")) {
            return normalizeLogName(logName.substring(1));
        } else {
            return logName;
        }
    }

    static URI getLogUri(URI namespace, String logName) {
        String scope = normalizeScope(namespace.getPath());
        String logPath = getLogPath(scope, normalizeLogName(logName));
        try {
            return new URI(
                namespace.getScheme(),
                namespace.getUserInfo(),
                namespace.getHost(),
                namespace.getPort(),
                logPath,
                namespace.getQuery(),
                namespace.getFragment());
        } catch (URISyntaxException e) {
            log.info("Invalid uri : namespace = {}, logName = {}", namespace, logName, e);
            throw new RuntimeException(
                "Invalid uri : namespace = " + namespace + ", logName = " + logName,
                e);
        }
    }

    static String getLogPrefixBeginKey(String scope, String logNamePrefix) {
        logNamePrefix = normalizeLogName(logNamePrefix);
        if (isBlank(logNamePrefix)) { // it is root
            return String.format("%s/%s%s", scope, LOGS_NODE, BEGIN_SEP);
        } else {
            return String.format("%s/%s/%s%s", scope, LOGS_NODE, logNamePrefix, BEGIN_SEP);
        }
    }

    static String getLogPrefixEndKey(String scope, String logNamePrefix) {
        logNamePrefix = normalizeLogName(logNamePrefix);
        if (isBlank(logNamePrefix)) { // it is root
            return String.format("%s/%s%s", scope, LOGS_NODE, END_SEP);
        } else {
            return String.format("%s/%s/%s%s", scope, LOGS_NODE, logNamePrefix, END_SEP);
        }
    }

    static String getLogsBeginKey(String scope) {
        return String.format("%s/%s%s", scope, LOGS_NODE, BEGIN_SEP);
    }

    static String getLogsEndKey(String scope) {
        return String.format("%s/%s%s", scope, LOGS_NODE, END_SEP);
    }

    static String getLogPath(String scope, String logName) {
        return String.format("%s/%s/%s", scope, LOGS_NODE, logName);
    }

    static String getLogName(String scope, String logPath) {
        return logPath.replace(getLogsBeginKey(scope), "");
    }

}
