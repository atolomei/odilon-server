package io.odilon.vfs;

import io.odilon.model.ObjectMetadata;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;

/**
*  
* @author atolomei@novamens.com (Alejandro Tolomei)
*/
public interface RAIDDeleteObjectHandler {

	/** Delete Object */
	public void delete(VFSBucket bucket, String objectName);
	
	/** Delete Version */
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta);
	public void deleteBucketAllPreviousVersions(VFSBucket bucket);
	public void wipeAllPreviousVersions();

	/** rollbackJournal */
	public void rollbackJournal(VFSOperation op, boolean recoveryMode);
	
	

	
}
