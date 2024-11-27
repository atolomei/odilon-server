package io.odilon.virtualFileSystem;

import io.odilon.model.BucketMetadata;
import io.odilon.model.RedundancyLevel;
import io.odilon.virtualFileSystem.model.IODriver;
import io.odilon.virtualFileSystem.model.JournalService;
import io.odilon.virtualFileSystem.model.LockService;
import io.odilon.virtualFileSystem.model.ServerBucket;

public abstract class BaseRAIDHandler {

	public abstract IODriver getDriver();

	public JournalService getJournalService() {
		return getDriver().getJournalService();
	}

	public LockService getLockService() {
		return getDriver().getLockService();
	}

	protected boolean isEncrypt() {
		return getDriver().isEncrypt();
	}

	public RedundancyLevel getRedundancyLevel() {
		return getDriver().getRedundancyLevel();
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

	protected void bucketReadLock(ServerBucket bucket) {
		getLockService().getBucketLock(bucket).readLock().lock();
	}

	protected void bucketReadUnlock(ServerBucket bucket) {
		getLockService().getBucketLock(bucket).readLock().unlock();
	}

	protected void bucketWriteLock(BucketMetadata meta) {
		getLockService().getBucketLock(meta.getId()).writeLock().lock();
	}

	protected void bucketWriteUnlock(BucketMetadata meta) {
		getLockService().getBucketLock(meta.getId()).writeLock().unlock();
	}

	protected String objectInfo(ServerBucket bucket) {
		return getDriver().objectInfo(bucket);
	}

}
