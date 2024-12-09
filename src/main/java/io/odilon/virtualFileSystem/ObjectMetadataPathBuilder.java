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

import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.SimpleDrive;

/**
 * 
 * Context: 
 * 
 * STORAGE
 * BACKUP
 * WORK
 * 
 * Version: head, previous (version_n)
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ObjectMetadataPathBuilder extends PathBuilder {

    private SimpleDrive drive;
    //private ObjectMetadata meta;
    private String objectName;
    ServerBucket bucket;
    private Context context;
    
    
    public ObjectMetadataPathBuilder(SimpleDrive drive, ServerBucket bucket, String objectName) {
            this(drive, bucket, objectName, Context.STORAGE);
    }
    
    
    public ObjectMetadataPathBuilder(SimpleDrive drive, ServerBucket bucket, String objectName, Context context) {
        this.drive=drive;
        this.objectName=objectName;
        this.bucket=bucket;
        this.context=context;
        
    }
    
    public String build() {

        if (getContext()==Context.STORAGE)
            return getDrive().getObjectDataFilePath(getBucket().getId(), getObjectName());
        
        return getDrive().getObjectDataFilePath(getBucket().getId(), getObjectName());
        
    }

    
    private ServerBucket getBucket() {
        return this.bucket;
    }

    private SimpleDrive getDrive() {
        return this.drive;
    }

    private String getObjectName() {
        return this.objectName;
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }
    
}
