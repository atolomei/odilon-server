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
package io.odilon.virtualFileSystem.raid1;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.model.ObjectMetadata;
import io.odilon.virtualFileSystem.model.ServerBucket;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDOneTransactionObjectHandler extends RAIDOneTransactionHandler {

    @JsonProperty("bucket")
    private final ServerBucket bucket;

    @JsonProperty("objectName")
    private final String objectName;

    public RAIDOneTransactionObjectHandler(RAIDOneDriver driver, ServerBucket bucket, String objectName) {
        super(driver);
        this.bucket = bucket;
        this.objectName = objectName;
    }

    protected ServerBucket getBucket() {
        return bucket;
    }

    protected String getObjectName() {
        return objectName;
    }

    protected void bucketReadLock() {
        bucketReadLock(getBucket());
    }

    protected void objectWriteLock() {
        objectWriteLock(getBucket(), getObjectName());
    }

    protected String info() {
        return objectInfo(getBucket(), getObjectName());
    }

    protected String info(String str) {
        return objectInfo(getBucket(), getObjectName(), str);
    }

    /** must be executed inside the critical zone. */
    protected ObjectMetadata getMetadata() {
        return getMetadata(getBucket(), getObjectName(), false);
    }

    /** must be executed inside the critical zone. */
    protected void objectWriteUnLock() {
        objectWriteUnLock(getBucket(), getObjectName());
    }

    /** must be executed inside the critical zone. */
    protected void bucketReadUnLock() {
        bucketReadUnLock(getBucket());
    }

    /** must be executed inside the critical zone. */
    protected void checkNotExistObject() {
        checkNotExistObject(getBucket(), getObjectName());
    }

    /** must be executed inside the critical zone. */
    protected void checkExistObject() {
        checkExistObject(getBucket(), getObjectName());
    }

    /** must be executed inside the critical zone. */
    protected void checkExistsBucket() {
        checkExistsBucket(getBucket());
    }

}