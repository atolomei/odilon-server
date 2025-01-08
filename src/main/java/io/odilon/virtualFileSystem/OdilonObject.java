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
package io.odilon.virtualFileSystem;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemObject;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;
import io.odilon.model.BaseObject;
import io.odilon.model.ObjectMetadata;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class OdilonObject extends BaseObject implements VirtualFileSystemObject {


    private ObjectMetadata objectMetadata;

    @JsonIgnore
    private ServerBucket bucket;

    @JsonIgnore
    private String objectName;

    @JsonIgnore
    private VirtualFileSystemService vfs;

    public OdilonObject(ServerBucket bucket, String objectName, VirtualFileSystemService vfs) {
        this.bucket = bucket;
        this.objectName = objectName;
        this.vfs = vfs;
    }

    @Override
    public ServerBucket getBucket() {
        return bucket;
    }

    @Override
    public ObjectMetadata getObjectMetadata() {
        if (this.objectMetadata == null)
            this.objectMetadata = this.vfs.getObjectMetadata(bucket, objectName);
        return this.objectMetadata;
    }

    public int hashCode() {
        return ((bucket != null ? bucket.getName() : "null") + ServerConstant.BO_SEPARATOR + objectName).hashCode();
    }

    @Override
    public String getObjectName() {
        return this.objectName;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return this.vfs.getObjectStream(bucket, objectName);
    }
}
