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

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.odilon.model.BaseObject;
import io.odilon.model.BucketMetadata;
import io.odilon.model.BucketStatus;
import io.odilon.virtualFileSystem.model.DriveBucket;
import io.odilon.virtualFileSystem.model.ServerBucket;

/**
 * <p>
 * Server representation of a Bucket. A bucket is like a folder, it just
 * contains binary objects, potentially a very large number.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@JsonInclude(Include.NON_NULL)
public class OdilonBucket extends BaseObject implements ServerBucket {

    @JsonProperty("bucketMetadata")
    final private BucketMetadata meta;

    public OdilonBucket(DriveBucket db) {
        this.meta = db.getBucketMetadata();
    }

    public OdilonBucket(BucketMetadata meta) {
        this.meta = meta;
    }

    @Override
    public boolean isAccesible() {
        return this.meta.status == BucketStatus.ENABLED || meta.status == BucketStatus.ARCHIVED;
    }

    @Override
    public String getName() {
        return this.meta.bucketName;
    }

    @Override
    public OffsetDateTime getCreationDate() {
        return this.meta.creationDate;
    }

    @Override
    public OffsetDateTime getLastModifiedDate() {
        return this.meta.lastModified;
    }

    @Override
    public BucketStatus getStatus() {
        return this.meta.status;
    }

    @Override
    public BucketMetadata getBucketMetadata() {
        return this.meta;
    }

    @Override
    public Long getId() {
        return this.meta.id;
    }
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(this.getClass().getSimpleName());
        str.append(toJSON());
        return str.toString();
    }
    
}
