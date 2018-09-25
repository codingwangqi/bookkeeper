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

package org.apache.bookkeeper.metadata.etcd.helpers;

import java.util.Iterator;
import java.util.List;

/**
 * Iterator to iterate items over a {@link KeyStream}.
 */
public class KeyStreamIterator<T> implements Iterator<T> {

    private final KeyIterator<T> ki;
    private Iterator<T> currentBatchIter = null;

    public KeyStreamIterator(KeyStream<T> stream) {
        this.ki = new KeyIterator<>(stream);
    }

    @Override
    public boolean hasNext() {
        try {
            if (null == currentBatchIter) {
                return ki.hasNext();
            } else {
                if (currentBatchIter.hasNext()) {
                    return true;
                } else {
                    return ki.hasNext();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T next() {
        try {
            if (null == currentBatchIter || !currentBatchIter.hasNext()) {
                List<T> currentBatch = ki.next();
                currentBatchIter = currentBatch.iterator();
            }
            return currentBatchIter.next();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
