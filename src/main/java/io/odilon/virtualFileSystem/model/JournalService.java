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
package io.odilon.virtualFileSystem.model;

import io.odilon.model.BucketMetadata;
import io.odilon.service.SystemService;

/**
 * <p>
 * Service for ensuring Data Integrity<br/>
 * Changes to data files must be written only after those changes have been
 * logged, that is, <br/>
 * after log records describing the changes have been flushed to permanent
 * storage. <br/>
 * This is roll-forward recovery, also known as REDO.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface JournalService extends SystemService {

    /**
     * ----------------- KEY ------------------
     */
    public VirtualFileSystemOperation saveServerKey();

    /**
     * ----------------- SERVER ------------------
     */
    public VirtualFileSystemOperation createServerMetadata();

    public VirtualFileSystemOperation updateServerMetadata();

    /**
     * ----------------- BUCKET ------------------
     */
    public VirtualFileSystemOperation createBucket(BucketMetadata meta);

    public VirtualFileSystemOperation updateBucket(ServerBucket bucket, String newBucketName);

    public VirtualFileSystemOperation deleteBucket(ServerBucket bucket);

    /**
     * ----------------- OJBECT ------------------
     */
    public VirtualFileSystemOperation createObject(ServerBucket bucket, String objectName);

    public VirtualFileSystemOperation updateObject(ServerBucket bucket, String objectName, int version);

    public VirtualFileSystemOperation updateObjectMetadata(ServerBucket bucket, String objectName, int version);

    /** Version control */
    public VirtualFileSystemOperation restoreObjectPreviousVersion(ServerBucket bucket, String objectName, int versionToRestore);

    public VirtualFileSystemOperation deleteObject(ServerBucket bucket, String objectName, int currentHeadVersion);

    public VirtualFileSystemOperation deleteObjectPreviousVersions(ServerBucket bucket, String objectName, int currentHeadVersion);

    /** sync new drive */
    public VirtualFileSystemOperation syncObject(ServerBucket bucket, String objectName);

    /** ----------------- */
    public boolean commit(VirtualFileSystemOperation operation);
    public boolean commit(VirtualFileSystemOperation operation, Object payload);
    
    public boolean cancel(VirtualFileSystemOperation operation);
    public boolean cancel(VirtualFileSystemOperation odilonVirtualFileSystemOperation, Object payload);
    
    public String newOperationId();



    /**
     * <p>
     * If there is a replica enabled, 1. save the op into the replica queue 2.
     * remove op from journal error -> remove from replication on recovery rollback
     * op -> 1. remove op from replica, remove op from local ops
     * </p>
     */
    

}
