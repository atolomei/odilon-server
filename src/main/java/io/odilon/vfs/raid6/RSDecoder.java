package io.odilon.vfs.raid6;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;
import io.odilon.util.Check;
import io.odilon.vfs.model.VirtualFileSystemService;

public class RSDecoder {

	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(RSEncoder.class.getName());

	private RAIDSixDriver driver;

    private final int data_shards;
    private final int parity_shards;
    private final int total_shards;

    
	protected RSDecoder(RAIDSixDriver driver) {
    	Check.requireNonNull(driver);
		this.driver=driver;
		this.data_shards = getVFS().getServerSettings().getRAID6DataDrives();
		this.parity_shards = getVFS().getServerSettings().getRAID6ParityDrives();
		this.total_shards = data_shards + parity_shards;  
		if (!driver.isConfigurationValid(data_shards, parity_shards))
			throw new InternalCriticalException("Incorrect configuration for " + driver.getRedundancyLevel().getName()+" -> data: " + String.valueOf(data_shards) + " | parity:" + String.valueOf(parity_shards));
	}
 	
	 /**
     * @param is
     */
    public File decode(String bucketName, String objectName) {
    	
    	Check.requireNonNull(bucketName);
    	Check.requireNonNull(objectName);
    	
    	int chunk = 0;
    	
    	boolean done = false;

    	File file = getVFS().getFileCacheService().get(bucketName, objectName); 
    	
    	if (file!=null) {
    		getDriver().getVFS().getSystemMonitorService().getCacheFileHitCounter().inc();
    		return file;
    	}
    	
    	getDriver().getVFS().getSystemMonitorService().getCacheFileMissCounter().inc();
		
    	getVFS().getFileCacheService().getLockService().getFileCacheLock(bucketName, objectName).writeLock().lock();

    	try {
	    	String tempPath = getVFS().getFileCacheService().getFileCachePath(bucketName, objectName);
	    	try (OutputStream out = new BufferedOutputStream(new FileOutputStream(tempPath))) {
	    		while (!done) {
	        		done = decodeChunk(bucketName, objectName, chunk++, out);
	        	}
	    	} catch (FileNotFoundException e) {
	    		throw new InternalCriticalException(e, "b:" + bucketName +  " | o:" + objectName + " | f:" +tempPath);
			} catch (IOException e) {
				throw new InternalCriticalException(e, "b:" + bucketName +  " | o:" + objectName + " | f:" +tempPath);
			}
	    	File decodedFile = new File(tempPath);
	    	getVFS().getFileCacheService().put(bucketName, objectName, decodedFile, false);
	    	return decodedFile;
	    	
    	} finally {
    		getVFS().getFileCacheService().getLockService().getFileCacheLock(bucketName, objectName).writeLock().unlock();
    	}
    }

    /**
     * @param bucketName
     * @param objectName
     * @param chunk
     * @param out
     * @return
     */
    public boolean decodeChunk(String bucketName, String objectName, int chunk, OutputStream out) {
    	
    	// Read in any of the shards that are present.
        // (There should be checking here to make sure the input
        // shards are the same size, but there isn't.)
    	
    	// BUFFER 3
        final byte [] [] shards = new byte [this.total_shards] [];
        final boolean [] shardPresent = new boolean [this.total_shards];
        
        int shardSize = 0;
        int shardCount = 0;
        
        for (int disk = 0; disk < this.total_shards; disk++) {
        	
        	String dirPath = getDriver().getDrivesEnabled().get(disk).getBucketObjectDataDirPath(bucketName);
        	File shardFile = new File(dirPath, objectName+ "." + String.valueOf(chunk) +"." + String.valueOf(disk));
           
            if (shardFile.exists()) {
                shardSize = (int) shardFile.length();
                
            	// BUFFER 4
                shards[disk] = new byte [shardSize];
                shardPresent[disk] = true;
                shardCount += 1;
    			try (InputStream in = new FileInputStream(shardFile)) {
					in.read(shards[disk], 0, shardSize);
				} catch (FileNotFoundException e) {
					throw new InternalCriticalException(e, "b:" + bucketName +  " | o:" + objectName + " | f:" + shardFile.getName());
				} catch (IOException e) {
					throw new InternalCriticalException(e, "b:" + bucketName +  " | o:" + objectName + " | f:" + shardFile.getName());
				}
            }
        }
        
        // We need at least DATA_SHARDS to be able to reconstruct the file.
        //
        if (shardCount < this.data_shards) {
        	throw new InternalCriticalException("We need at least " + String.valueOf(this.data_shards)+ " shards to be able to reconstruct the file | b:" + bucketName +  " | o:" + objectName);
        }
        
        // Make empty buffers for the missing shards.
        //
        for (int i = 0; i < this.total_shards; i++) {
            if (!shardPresent[i]) {
            	// BUFFER 5
            	shards[i] = new byte [shardSize];
            }
        }
        
        // Use Reed-Solomon to fill in the missing shards
        //
        ReedSolomon reedSolomon = new ReedSolomon(this.data_shards, this.parity_shards);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        
        // Combine the data shards into one buffer for convenience.
        // (This is not efficient, but it is convenient.)
        
    	// BUFFER 6
        byte [] allBytes = new byte [shardSize * this.data_shards];
        for (int i = 0; i < this.data_shards; i++) {
            System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
        }

        // Extract the file length
        int fileSize = ByteBuffer.wrap(allBytes).getInt();

        try {
			out.write(allBytes, ServerConstant.BYTES_IN_INT, fileSize);
		} catch (IOException e) {
            throw  new InternalCriticalException(e,  "b:" + bucketName +  " | o:" + objectName);
		}

		if (shardSize<( ServerConstant.MAX_CHUNK_SIZE - ServerConstant.BYTES_IN_INT))
			return true;
		
    	return false;
    }
    
    
	
	public RAIDSixDriver getDriver() {
		return this.driver;
	}
	
	public VirtualFileSystemService getVFS() {
		return this.driver.getVFS();
	}
}
