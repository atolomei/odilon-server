package io.odilon.vfs;

import io.odilon.model.ObjectMetadata;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;

public interface RAIDDeleteObjectHandler {

	/** Delete Object */
	public void delete(VFSBucket bucket, String objectName);
	
	/** Delete Version */
	public void deleteObjectAllPreviousVersions(ObjectMetadata meta);
	public void deleteBucketAllPreviousVersions(VFSBucket bucket);
	public void wipeAllPreviousVersions();

	/** rollbackJournal */
	public void rollbackJournal(VFSOperation op, boolean recoveryMode);
	
	/**  
	 *  Post transaction
	 *  <p>executed Async by a {@link ServiceRequest} from the {@link SchedulerService}</p>
	 * */
	//public void postObjectDelete(ObjectMetadata meta, int headVersion);
	//public void postObjectPreviousVersionDeleteAll(ObjectMetadata meta, int headVersion);

	
}
