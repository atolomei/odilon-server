package io.odilon.vfs;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.util.Check;
import io.odilon.vfs.model.SimpleDrive;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>For RAID 0 and RAID 1</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@NotThreadSafe
@Component
@Scope("prototype")
public class ODSimpleDrive extends ODDrive implements SimpleDrive {
			
	
	static private Logger logger = Logger.getLogger(ODSimpleDrive.class.getName());
	
	/**
	 * Constructor called by Spring.io 
	 * @param rootDir
	 */
	@Autowired
	protected ODSimpleDrive(String rootDir) {
		super(rootDir);
	}

	/**
	 * 
	 * <p>Constructor explicit
	 * 
	 * @param name
	 * @param rootDir
	 */
	protected ODSimpleDrive(String name, String rootDir, int configOrder) {
		super(name, rootDir, configOrder);
	}
	
	/**
	 * <p></p>
	 */	
	@Override
	public String getObjectDataFilePath(String bucketName, String objectName) {
		return this.getRootDirPath() + File.separator + bucketName + File.separator + objectName;
	}
	
	@Override
	public String getObjectDataVersionFilePath(String bucketName, String objectName, int version) {
		return this.getRootDirPath() + File.separator + bucketName + File.separator + VirtualFileSystemService.VERSION_DIR + File.separator + objectName + VirtualFileSystemService.VERSION_EXTENSION + String.valueOf(version);
	}
	
	/**
	 * <b>Object Data</b>
	 * 
	 * 
	 * <p>IMPORTANT: 
	 * Stream is not closed by this method
	 * This method is not ThreadSafe
	 * </p>
	 */
	@Override
	public InputStream getObjectInputStream(String bucketName, String objectName) {

		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null -> b:" + bucketName);
		
		try {
			return Files.newInputStream(getObjectDataFile(bucketName, objectName).toPath());
		} catch (Exception e) {
			throw new InternalCriticalException(e, "b:" +  bucketName + ", o:" + objectName +", d:" + getName());
		}
	}
	
	/**
	 * <b>bDATA</b> 
	 * 
	 * Dir -> directory
	 * Path -> to File
	 * File -> File
	 * 
	 * <p>IMPORTANT: Stream is not closed by this method</p>
	 */							
	@Override
	public File putObjectStream(String bucketName, String objectName, InputStream stream) throws IOException {

		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null -> b:" + bucketName);
		if (!super.existBucketMetadataDir(bucketName))
			  throw new IllegalStateException("Bucket Metadata Directory must exist -> d:" + getName() + " | b:" + bucketName);
		
		/** data Bucket created on demand */
		createDataBucketDirIfNotExists(bucketName);
		
		try {

			String dataFilePath = this.getObjectDataFilePath(bucketName, objectName); 
			transferTo(stream, dataFilePath);
			
			return new File(dataFilePath);
		}
		 catch (IOException e) {
			logger.error(e.getClass().getName() + " putObjectStream -> " + "b:" +  bucketName + ", o:" + objectName +", d:" + getName());
			throw (e);
		}
	}
	
	@Override
	public void putObjectDataFile(String bucketName, String objectName, File objectFile) throws IOException {
		try (InputStream is = new FileInputStream(objectFile)) {
			putObjectStream(bucketName, objectName, is);
		}
	}
	
	@Override				
	public void putObjectDataVersionFile(String bucketName, String objectName, int version, File objectFile) throws IOException {
		try (InputStream is = new BufferedInputStream(new FileInputStream(objectFile))) {
			putObjectDataVersionStream(bucketName, objectName, version, is);
		}
	}

	@Override
	public File getObjectDataFile(String bucketName, String objectName) {
		return new File(this.getRootDirPath(), bucketName + File.separator + objectName);
	}

	@Override
	public File getObjectDataVersionFile(String bucketName, String objectName, int version) {
		return new File(getObjectDataVersionFilePath(bucketName, objectName, version));
	}

	protected File putObjectDataVersionStream(String bucketName, String objectName, int version, InputStream stream) throws IOException {

		Check.requireNonNullArgument(bucketName, "bucketName is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null -> b:" + bucketName);
		
		try {
			String dataFilePath = this.getObjectDataVersionFilePath(bucketName, objectName, version); 
			transferTo(stream, dataFilePath);
			return new File(dataFilePath);
		}
		 catch (IOException e) {
				logger.error(e.getClass().getName() + " getObjectDataVersionFile -> " + "b:" +  bucketName + ", o:" + objectName +", d:" + getName());			
				throw (e);
		}
	}
	
}
