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
import io.odilon.rs.ReedSolomon;
import io.odilon.util.Check;
import io.odilon.vfs.model.VirtualFileSystemService;

public class RSDecoder {

	static private Logger logger = Logger.getLogger(RSEncoder.class.getName());

	private RAIDSixDriver driver;

    private final int DATA_SHARDS;
    private final int PARITY_SHARDS;
    private final int TOTAL_SHARDS;

    
	protected RSDecoder(RAIDSixDriver driver) {
		this.driver=driver;
		
		this.DATA_SHARDS = getVFS().getServerSettings().getRAID6DataDrives();
		this.PARITY_SHARDS = getVFS().getServerSettings().getRAID6ParityDrives();
		this.TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;  
		
		if (DATA_SHARDS!=4 || PARITY_SHARDS!=2) {
			throw new InternalCriticalException("Incorrect configuration for RAID 6 -> data: " + String.valueOf(DATA_SHARDS) + " | parity:" + String.valueOf(PARITY_SHARDS));
		}
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
    	
    	if (file!=null)
    		return file;
    	
    	
    	getVFS().getFileCacheService().getLockService().getFileCacheLock(bucketName, objectName).writeLock().lock();

    	try {
    	
	    	String tempPath = getVFS().getFileCacheService().getFileCachePath(bucketName, objectName);
	    	
	    	try (OutputStream out = new BufferedOutputStream(new FileOutputStream(tempPath))) {
	    		while (!done) {
	        		done = decodeChunk(bucketName, objectName, chunk++, out);
	        	}
	    		
	    	} catch (FileNotFoundException e) {
	    		throw new InternalCriticalException(e, "o:" + objectName);
	    		
			} catch (IOException e) {
				throw new InternalCriticalException(e, "o:" + objectName);
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
    	
    	
    	// BUFFER
        final byte [] [] shards = new byte [TOTAL_SHARDS] [];
        final boolean [] shardPresent = new boolean [TOTAL_SHARDS];
        
        int shardSize = 0;
        int shardCount = 0;
        
        for (int disk = 0; disk < TOTAL_SHARDS; disk++) {
        	
        	String dirPath = getDriver().getDrivesEnabled().get(disk).getBucketObjectDataDirPath(bucketName);
        	File shardFile = new File(dirPath, objectName+ "." + String.valueOf(chunk) +"." + String.valueOf(disk));
           
            if (shardFile.exists()) {
                shardSize = (int) shardFile.length();
                
            	// BUFFER
                shards[disk] = new byte [shardSize];
                shardPresent[disk] = true;
                shardCount += 1;
    			try (InputStream in = new FileInputStream(shardFile)) {
					in.read(shards[disk], 0, shardSize);
				} catch (FileNotFoundException e) {
					logger.error(e);
					System.exit(1);
				} catch (IOException e) {
					logger.error(e);
					System.exit(1);
				}
            }
        }
        
        // We need at least DATA_SHARDS to be able to reconstruct the file.
        //
        if (shardCount < DATA_SHARDS) {
        	throw new InternalCriticalException("We need at least DATA_SHARDS to be able to reconstruct the file | o:" + objectName);
        }
        
        // Make empty buffers for the missing shards.
        //
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            if (!shardPresent[i]) {
            	// BUFFER
            	shards[i] = new byte [shardSize];
            }
        }
        
        // Use Reed-Solomon to fill in the missing shards
        //
        ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

        
        // Combine the data shards into one buffer for convenience.
        // (This is not efficient, but it is convenient.)
        
    	// BUFFER
        byte [] allBytes = new byte [shardSize * DATA_SHARDS];
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
        }

        // Extract the file length
        int fileSize = ByteBuffer.wrap(allBytes).getInt();

        try {
			out.write(allBytes,  ServerConstant.BYTES_IN_INT, fileSize);
		} catch (IOException e) {
            throw  new InternalCriticalException("Not enough shards present");
		}

		if (shardSize<( ServerConstant.MAX_CHUNK_SIZE -  ServerConstant.BYTES_IN_INT))
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
