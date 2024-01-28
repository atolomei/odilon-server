package io.odilon.vfs.model;

import java.io.IOException;
import java.io.InputStream;

import io.odilon.model.ObjectMetadata;

public interface VFSObject {
	
	public String getObjectName();
	public VFSBucket getBucket();
	public ObjectMetadata getObjectMetadata();
	public InputStream getInputStream() throws IOException;
	
}
