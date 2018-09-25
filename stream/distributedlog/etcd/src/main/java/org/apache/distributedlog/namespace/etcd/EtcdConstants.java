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

import com.coreos.jetcd.data.ByteSequence;

/**
 * Constants used in the etcd namespace driver.
 */
final class EtcdConstants {

    private EtcdConstants() {}

    public static final String BEGIN_SEP = "/";
    public static final String END_SEP = "0";

    public static final String LOGS_NODE = "_logs_";

    public static final ByteSequence EMPTY_BS  = ByteSequence.fromBytes(new byte[0]);

}
