/*
 * Odilon Object Storage
 * (C) Novamens 
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
package io.odilon.virtualFileSystem.raid0;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.virtualFileSystem.ObjectPath;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 *
 * <p>RAID 0. base rollback Handler for Object's operations</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public abstract class RAIDZeroRollbackObjectHandler extends RAIDZeroRollbackHandler {

    @JsonProperty("path")
    private ObjectPath path;

    public RAIDZeroRollbackObjectHandler(RAIDZeroDriver driver, VirtualFileSystemOperation operation, boolean recovery) {
        super(driver, operation, recovery);
    }

    protected ObjectPath getObjectPath() {
        if (this.path == null) {
            ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
            String objectName = getOperation().getObjectName();
            this.path = new ObjectPath(getWriteDrive(bucket, objectName), bucket, objectName);
        }
        return this.path;
    }
}
