package io.odilon.vfs;

import java.io.InputStream;

import io.odilon.model.ObjectMetadata;
import io.odilon.vfs.model.VFSBucket;
import io.odilon.vfs.model.VFSOperation;

public interface RAIDUpdateObjectHandler extends  RAIDHandler {
	
	public void update(VFSBucket bucket, String objectName, InputStream stream, String srcFileName, String contentType);
	public ObjectMetadata restorePreviousVersion(VFSBucket bucket, String objectName);
	
	public void updateObjectMetadata(ObjectMetadata meta);
	public void rollbackJournal(VFSOperation op, boolean recoveryMode);
	public void onAfterCommit(VFSBucket bucket, String objectName, int previousVersion, int currentVersion);
	
	
}
