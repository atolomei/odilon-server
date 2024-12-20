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

import org.springframework.lang.NonNull;

import io.odilon.encryption.EncryptionService;
import io.odilon.model.BucketMetadata;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.RedundancyLevel;
import io.odilon.replication.ReplicationService;
import io.odilon.service.ServerSettings;
import io.odilon.virtualFileSystem.model.IODriver;
import io.odilon.virtualFileSystem.model.JournalService;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

public abstract class BaseRAIDHandler {

    public abstract IODriver getDriver();

    public VirtualFileSystemService getVirtualFileSystemService() {
        return getDriver().getVirtualFileSystemService();
    }

    protected ServerSettings getServerSettings() {
        return getVirtualFileSystemService().getServerSettings();
    }

    protected BucketCache getBucketCache() {
        return getVirtualFileSystemService().getBucketCache();
    }

    /**
     * This check must be executed inside the critical section
     */
    protected ServerBucket getCacheBucket(Long bucketId) {
        return getVirtualFileSystemService().getBucketCache().get(bucketId);
    }

    /**
     * This check must be executed inside the critical section
     */
    protected boolean existsCacheBucket(String bucketName) {
        return getBucketCache().contains(bucketName);
    }

    /**
     * This check must be executed inside the critical section
     */
    protected boolean existsCacheBucket(Long id) {
        return getBucketCache().contains(id);
    }

    /**
     * This check must be executed inside the critical section
     */
    protected boolean existsCacheBucket(ServerBucket bucket) {
        return getBucketCache().contains(bucket);
    }

    protected EncryptionService getEncryptionService() {
        return getVirtualFileSystemService().getEncryptionService();
    }

    public JournalService getJournalService() {
        return getDriver().getJournalService();
    }

    public LockService getLockService() {
        return getDriver().getLockService();
    }

    protected boolean isEncrypt() {
        return getDriver().isEncrypt();
    }

    protected ReplicationService getReplicationService() {
        return getVirtualFileSystemService().getReplicationService();
    }

    protected boolean isStandByEnabled() {
        return getVirtualFileSystemService().getServerSettings().isStandByEnabled();
    }

    public RedundancyLevel getRedundancyLevel() {
        return getDriver().getRedundancyLevel();
    }

    protected String opInfo(VirtualFileSystemOperation op) {
        return getDriver().opInfo(op);
    }

    protected String objectInfo(ServerBucket bucket, String objectName, String srcFileName) {
        return getDriver().objectInfo(bucket, objectName, srcFileName);
    }

    protected String objectInfo(String bucketName, String objectName, String srcFileName) {
        return getDriver().objectInfo(bucketName, objectName, srcFileName);
    }

    protected String objectInfo(ObjectMetadata meta) {
        return getDriver().objectInfo(meta);
    }

    protected String objectInfo(@NonNull ServerBucket bucket, @NonNull String objectName) {
        return getDriver().objectInfo(bucket, objectName);
    }

    protected void objectReadLock(ServerBucket bucket, String objectName) {
        getLockService().getObjectLock(bucket, objectName).readLock().lock();
    }

    protected void objectReadUnLock(ServerBucket bucket, String objectName) {
        getLockService().getObjectLock(bucket, objectName).readLock().unlock();
    }

    protected void objectWriteLock(ServerBucket bucket, String objectName) {
        getLockService().getObjectLock(bucket, objectName).writeLock().lock();
    }

    protected void objectWriteUnLock(ServerBucket bucket, String objectName) {
        getLockService().getObjectLock(bucket, objectName).writeLock().unlock();
    }

    protected void objectWriteLock(Long bucketId, String objectName) {
        getLockService().getObjectLock(bucketId, objectName).writeLock().lock();
    }

    protected void objectWriteUnLock(Long bucketId, String objectName) {
        getLockService().getObjectLock(bucketId, objectName).writeLock().unlock();
    }

    protected void objectWriteLock(ObjectMetadata meta) {
        getLockService().getObjectLock(meta.getBucketId(), meta.getObjectName()).writeLock().lock();
    }

    protected void objectWriteUnLock(ObjectMetadata meta) {
        getLockService().getObjectLock(meta.getBucketId(), meta.getObjectName()).writeLock().unlock();
    }

    protected void bucketReadLock(String bucketName) {
        getLockService().getBucketLock(bucketName).readLock().lock();
    }

    protected void bucketReadLock(Long bucketId) {
        getLockService().getBucketLock(bucketId).readLock().lock();
    }

    protected void bucketReadUnLock(Long bucketId) {
        getLockService().getBucketLock(bucketId).readLock().unlock();
    }

    protected void bucketReadUnLock(String bucketName) {
        getLockService().getBucketLock(bucketName).readLock().lock();
    }

    protected void bucketReadLock(ServerBucket bucket) {
        getLockService().getBucketLock(bucket).readLock().lock();
    }

    protected void bucketReadUnLock(ServerBucket bucket) {
        getLockService().getBucketLock(bucket).readLock().unlock();
    }

    protected void bucketWriteLock(BucketMetadata meta) {
        getLockService().getBucketLock(meta).writeLock().lock();
    }

    protected void bucketWriteUnLock(BucketMetadata meta) {
        getLockService().getBucketLock(meta).writeLock().unlock();
    }

    protected String objectInfo(ServerBucket bucket) {
        return getDriver().objectInfo(bucket);
    }

    protected boolean isUseVaultNewFiles() {
        return getVirtualFileSystemService().isUseVaultNewFiles();
    }
}
