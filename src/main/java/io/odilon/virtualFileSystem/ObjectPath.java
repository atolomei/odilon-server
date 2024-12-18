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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

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
public class ObjectPath extends PathBuilder {

    private final Drive drive;
    
    private final String objectName;
    
    private final Long bucketId;
    
    
    public ObjectPath(Drive drive, ServerBucket bucket, String objectName) {
        this.drive=drive;
        this.objectName=objectName;
        this.bucketId=bucket.getId();
    }
    
    public ObjectPath(Drive drive, Long bucketId, String objectName) {
        this.drive=drive;
        this.objectName=objectName;
        this.bucketId=bucketId;
    }
    
    public ObjectPath(Drive drive, ObjectMetadata meta) {
        this.drive=drive;
        this.objectName=meta.getObjectName();
        this.bucketId=meta.getBucketId();
    }
    
    
    public Path metadataDirPath() {
            return metadataDirPath( Context.STORAGE);
    }
    
    public Path metadataDirPath(Context context) {
        return Paths.get(getBucketsDirPath()).resolve(getBucketId().toString() + File.separator + getObjectName());    
    }

    public Path metadataFilePath() {
            return metadataFilePath(Context.STORAGE);
    }
    
    public Path metadataFilePath(Context context) {
        return metadataDirPath(context).resolve(getObjectName() + ServerConstant.JSON);
    }
    
    public String getBucketsDirPath() {
        return getDrive().getBucketsDirPath();
    }
    
    /**
     * this works for RAID 1 and RAID 0
     * 
     * @param context
     * @param isHead
     * @return
     */
    
    public Path dataFilePath() {
        return dataFilePath(Context.STORAGE);
    }
    
    public Path dataFilePath(Context context) {
        if (context==Context.STORAGE)
            return Paths.get(getDrive().getRootDirPath(), getBucketId().toString() + File.separator + getObjectName());
        else
            throw new RuntimeException("not done");
    }
    
    public Path dataFilePath(Context context, int version) {
        if (context==Context.STORAGE) 
            return Paths.get(getDrive().getRootDirPath() + File.separator + getBucketId().toString() + File.separator + VirtualFileSystemService.VERSION_DIR, getObjectName() + VirtualFileSystemService.VERSION_EXTENSION + String.valueOf(version));
        else
            throw new RuntimeException("not done");
    }

    
    
    private Long getBucketId() {
        return this.bucketId;
    }

    private String getObjectName() {
        return this.objectName;
    }
    
    private Drive getDrive() {
        return this.drive;
    }


    
    //private String getObjectMetadataDir() {
     //   return getBucketsDir() + File.separator + getObjectMetadata().getBucketId().toString() + File.separator + getObjectMetadata().getObjectName();
    //}
    //private String getBucketsDir() {
     //       return getDrive().getRootDirPath() + File.separator + VirtualFileSystemService.SYS + File.separator + VirtualFileSystemService.BUCKETS;
    //}
    
    
    /**
    public Path build() {
       
       // String dir = getDrive().getObjectMetadataDirPath(getObjectMetadata().getBucketId(), getObjectMetadata().getObjectName());
        //String dir ="";
        
        //if (getContext()==Context.STORAGE) {
            if (isHead())
                return Paths.get(dir, getObjectMetadata().getObjectName() + ServerConstant.JSON);
            else
                return Paths.get(dir, getObjectMetadata().getObjectName() + VirtualFileSystemService.VERSION_EXTENSION + String.valueOf(getObjectMetadata().getVersion()) + ServerConstant.JSON);
        //}
        
        //return Paths.get(dir, getObjectMetadata().getObjectName() + ServerConstant.JSON);
        
    }
**/
    
   


    
}
