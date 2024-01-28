package io.odilon.vfs.model;

import java.time.OffsetDateTime;

import io.odilon.model.RedundancyLevel;
 

public interface VFSOperation {

	public VFSop getOp();
	
	public boolean commit();
	public boolean cancel();

	public String getId();
	public String toJSON();
		
	public OffsetDateTime getTimestamp();
	public RedundancyLevel getRedundancyLevel();
	public String getObjectName();
	public String getBucketName();
	
	public int getVersion();

	String getUUID();
	
	
}
