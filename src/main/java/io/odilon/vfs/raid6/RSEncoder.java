package io.odilon.vfs.raid6;

import java.io.BufferedOutputStream;
import java.io.File;
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

public class RSEncoder {

	static private Logger logger = Logger.getLogger(RSEncoder.class.getName());

	private RAIDSixDriver driver;

	private long fileSize = 0;
	private int chunk = 0;
    

    private final int data_shards;
    private final int partiy_shards;
    private final int total_shards;

    private RSFileBlocks encodedInfo;
    
	protected RSEncoder(RAIDSixDriver driver) {
		
		this.driver=driver;
		
		this.data_shards = getVFS().getServerSettings().getRAID6DataDrives();
		this.partiy_shards = getVFS().getServerSettings().getRAID6ParityDrives();
		this.total_shards = data_shards + partiy_shards;  
		
		if (!driver.isConfigurationValid(data_shards, partiy_shards))
			throw new InternalCriticalException("Incorrect configuration for RAID 6 -> data: " + String.valueOf(data_shards) + " | parity:" + String.valueOf(partiy_shards));
	}
	
	/**
	 * @param is
	 * @param bucketName
	 * @param objectName
	 * @return the size of the source file in bytes (note that the disk used by the shards is more (16 bytes for the file size plus the padding to make every shard 
	 * multiple of 4)
	 */
	public RSFileBlocks encode (InputStream is, String bucketName, String objectName) {
		
		Check.requireNonNull(is);
    	Check.requireNonNull(objectName);
    	
    	this.fileSize = 0;
    	this.chunk = 0;
    	this.encodedInfo = new RSFileBlocks();
    	
    	boolean done = false;
    	
    	try (is) {
	    	while (!done) { 
	    		done = encodeChunk(is, bucketName, objectName, chunk++);
	    	}
	    } catch (Exception e) {
	    		logger.error(e);
	    		throw new InternalCriticalException(e, "o:" + objectName);
	    }
    	
    	this.encodedInfo.fileSize=this.fileSize;
    	return this.encodedInfo;
	}
	
	/**
     * 
     * @param is
     */
    public boolean encodeChunk(InputStream is, String bucketName, String objectName, int chunk) {

    	// BUFFER 1
    	final byte [] allBytes = new byte[ ServerConstant.MAX_CHUNK_SIZE ];
    	int totalBytesRead=0;
    	boolean eof = false;
    	try {
    		final int maxBytesToRead = ServerConstant.MAX_CHUNK_SIZE - ServerConstant.BYTES_IN_INT;
    		boolean done = false;
    		int bytesRead = 0;
			while(!done) {
				bytesRead = is.read(allBytes, ServerConstant.BYTES_IN_INT+totalBytesRead, maxBytesToRead-totalBytesRead);
				if (bytesRead>0) 
					totalBytesRead += bytesRead;
				else 
					eof = true;
				done = eof || (totalBytesRead==maxBytesToRead);
			}
		} catch (IOException e) {
			throw new InternalCriticalException(e, " reading inputStream | b:" +bucketName + ", o:" + objectName);
		}

		if (totalBytesRead==0) 
			return true;
		
		this.fileSize += totalBytesRead;

		ByteBuffer.wrap(allBytes).putInt(totalBytesRead);
		
		final int storedSize = totalBytesRead + ServerConstant.BYTES_IN_INT;
		final int shardSize = (storedSize + data_shards - 1) / data_shards;
		
    	// BUFFER 2
		byte [] [] shards = new byte [total_shards] [shardSize];
		
        // Fill in the data shards
        for (int i = 0; i < data_shards; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }
                
        // Use Reed-Solomon to calculate the parity.
        ReedSolomon reedSolomon = new ReedSolomon(data_shards, partiy_shards);
        reedSolomon.encodeParity(shards, 0, shardSize);
        
        // Write out the resulting files.
        for (int block = 0; block < total_shards; block++) {
        	String dirPath = getDriver().getDrivesEnabled().get(block).getBucketObjectDataDirPath(bucketName);
        	String name = objectName+ "." + String.valueOf(chunk) +"." + String.valueOf(block);
        	File outputFile = new File(dirPath, name);
        							
			try  (OutputStream out = new BufferedOutputStream(new FileOutputStream(outputFile))) {
				out.write(shards[block]);
	        } catch (FileNotFoundException e) {
				throw new InternalCriticalException(e, "b:" +bucketName + ", o:" + objectName +" f: " + name);
			} catch (IOException e) {
				throw new InternalCriticalException(e, "b:" +bucketName + ", o:" + objectName +" f: " + name);
			}
			this.encodedInfo.encodedBlocks.add(outputFile);
        }
		return eof;
    }
    
	public RAIDSixDriver getDriver() {
		return this.driver;
	}

	public VirtualFileSystemService getVFS() {
		return this.driver.getVFS();
	}
	
}
