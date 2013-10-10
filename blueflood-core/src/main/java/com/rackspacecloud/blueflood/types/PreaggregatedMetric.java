/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.types;

import com.rackspacecloud.blueflood.utils.TimeValue;

/** It's like a rollup already. */
public class PreaggregatedMetric implements IMetric {
    private final long collectionTime;
    private final Locator locator;
    private final TimeValue ttl;
    private final Rollup value; // todo: does this need to be a specific type?
    
    public PreaggregatedMetric(long collectionTime, Locator locator, TimeValue ttl, Rollup value) {
        this.collectionTime = collectionTime;
        this.locator = locator;
        this.ttl = ttl;
        this.value = value;
    }
    
    public Locator getLocator() { return locator; }
    public long getCollectionTime() { return collectionTime; }
    public int getTtlInSeconds() { return (int)ttl.toSeconds(); }
    public Object getValue() { return value; }
}
