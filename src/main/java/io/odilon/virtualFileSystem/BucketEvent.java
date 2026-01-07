/*
 * Odilon Object Storage
 * (c) kbee 
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

package io.odilon.virtualFileSystem;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.service.BaseEvent;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class BucketEvent extends BaseEvent {

    private static final long serialVersionUID = 1L;

    @JsonIgnore
    static private Logger logger = Logger.getLogger(BucketEvent.class.getName());

    private final ServerBucket bucket;

    public BucketEvent(VirtualFileSystemOperation operation, Action action, ServerBucket bucket) {
        super(operation, action);
        this.bucket = bucket;
    }

    public ServerBucket getBucket() {
        return this.bucket;
    }

}
