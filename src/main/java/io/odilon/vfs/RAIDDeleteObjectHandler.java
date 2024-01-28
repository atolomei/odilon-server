package io.odilon.vfs;

import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;

public interface RAIDDeleteObjectHandler {

	/** Delete Object */
	public void delete(VFSBucket bucket, String objectName);
	
	/** Delete Version */
	public void deleteObjectAllPreviousVersions(VFSBucket bucket, String objectName);
	public void deleteBucketAllPreviousVersions(VFSBucket bucket);
	public void wipeAllPreviousVersions();
	

	/** rollbackJorunal */
	public void rollbackJournal(VFSOperation op, boolean recoveryMode);
	
	/** Post transaction */
	public void postObjectDeleteTransaction(String bucketName, String objectName, int headVersion);
	public void postObjectPreviousVersionDeleteAllTransaction(String bucketName, String objectName, int headVersion);
	
}
