package io.odilon.vfs.model;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;



public interface SimpleDrive extends Drive {
	
	public InputStream getObjectInputStream(String bucketName, String objectName);
	public File putObjectStream(String bucketName, String objectName, InputStream stream) throws IOException;
	public void putObjectDataFile(String bucketName, String objectName, File objectFile) throws IOException;
	public void putObjectDataVersionFile(String bucketName, String objectName, int version, File objectFile) throws IOException;

	public void deleteObjectMetadata(String bucketName, String objectName);
	
	public File getObjectDataFile(String bucketName, String objectName);
	public File getObjectDataVersionFile(String bucketName, String objectName, int version);
	
	public String getObjectDataFilePath			(String bucketName, String objectName);
	public String getObjectDataVersionFilePath	(String bucketName, String objectName, int version);

	

	

		
	
}


//public void deleteObject(String bucketName, String objectName);