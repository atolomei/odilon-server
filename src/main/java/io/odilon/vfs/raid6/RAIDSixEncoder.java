package io.odilon.vfs.raid6;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.util.Check;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * <p>RAID 6 Reed Solomon encoder</p> 
 * <p>Encodes {@link InputStream} into multiple block files in 
 * the File System using {@link https://en.wikipedia.org/wiki/Erasure_code}.</p>
 *  
 * @author atolomei@novamens.com (Alejandro Tolomei)  
 */
public class RAIDSixEncoder {

	@SuppressWarnings("unused")
	static private Logger logger = Logger.getLogger(RAIDSixEncoder.class.getName());

	private RAIDSixDriver driver;

	private long fileSize = 0;
	private int chunk = 0;
    

    private final int data_shards;
    private final int partiy_shards;
    private final int total_shards;

    private RAIDSixBlocks encodedInfo;
    
    private List<Drive> zDrives;
    

    /**
     * <p>Used by {@link RAIDSixDrive}, can not be created directly.</p> 
     * */
    
    protected RAIDSixEncoder(RAIDSixDriver driver) {
    	this(driver, null);
    }
    
    /**
     * <p>We use drivesAll to encode, assuming that drives that are in state {@link DriveStatys.NOT_SYNC}
     * are in the process of becoming enabled (via an async process in {@link RAIDSixDriveSync}.
     * </p>
     * 
     * <p>Used by {@link RAIDSixDrive}, can not be created directly.</p> 
     * */
	protected RAIDSixEncoder(RAIDSixDriver driver, List<Drive> udrives) {
		
		Check.requireNonNull(driver);
		
		this.driver = driver;
		this.zDrives = (udrives!=null) ? udrives : driver.getDrivesAll();
		
		this.data_shards = getVFS().getServerSettings().getRAID6DataDrives();
		this.partiy_shards = getVFS().getServerSettings().getRAID6ParityDrives();
		this.total_shards = data_shards + partiy_shards;  
	}
	
	/**
	 * <p> We can not use the {@link ObjectMetadata} here because <b>it may not exist yet</b>. 
	 *  The steps to upload objects are: <br/>
	 *  <ul>
	 * <li>upload binary data</li>
	 * <li>create ObjectMetadata </li>
	 * </ul>
	 * </p>
	 * 
	 * <p> 
	 * <b>Block naming convention Head version</b><br/>
	 * objectName.[block].[disk]<br/>
	 * <br/>
	 * <b>Previous version</b><br/>
	 * objectName.[block].[disk].v[version]<br/>
	 *</p>
	 * 
	 * @param is
	 * @param bucketName
	 * @param objectName
	 * 
	 * @return the size of the source file in bytes (note that the disk used by the shards 
	 * is more (16 bytes for the file size plus the padding to make every shard multiple of 4)
	 */
	
	public RAIDSixBlocks encodeHead (InputStream is, String bucketName, String objectName) {
		return encode(is, bucketName, objectName, Optional.empty());
	}
	
	public RAIDSixBlocks encodeVersion (InputStream is, String bucketName, String objectName, int version) {
		return encode(is, bucketName, objectName, Optional.of(version));
		
	}
	
	protected RAIDSixBlocks encode (InputStream is, String bucketName, String objectName, Optional<Integer> version ) {
		
		Check.requireNonNull(is);
		
    	Check.requireNonNull(objectName);
    	Check.requireNonNull(bucketName);
    	
    	if (!driver.isConfigurationValid(data_shards, partiy_shards))
			throw new InternalCriticalException("Incorrect configuration for RAID 6 -> data: " + String.valueOf(data_shards) + " | parity:" + String.valueOf(partiy_shards));
		
		if (getDrives().size()<total_shards)
			throw new InternalCriticalException("There are not enough drives to encode the file in RAID 6 -> " + String.valueOf(getDrives().size()) +" | required: " + String.valueOf(total_shards));
		
    	this.fileSize = 0;
    	this.chunk = 0;
    	this.encodedInfo = new RAIDSixBlocks();
    	
    	boolean done = false;
    	
    	try (is) {

    		while (!done)  
	    		done = encodeChunk(is, bucketName, objectName, chunk++, version);
	    	
	    } catch (Exception e) {
	    		throw new InternalCriticalException(e, "o:" + objectName);
	    }
    	
    	this.encodedInfo.fileSize=this.fileSize;
    	return this.encodedInfo;
	}
	
	/**
     * 
     * @param is
     */
    public boolean encodeChunk(InputStream is, String bucketName, String objectName, int chunk, Optional<Integer> o_version) {

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
		
        /** Fill in the data shards */
        for (int i = 0; i < data_shards; i++) 
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);

        
        /** Use Reed-Solomon to calculate the parity. */
        ReedSolomon reedSolomon = new ReedSolomon(data_shards, partiy_shards);
        reedSolomon.encodeParity(shards, 0, shardSize);

        /**
         * Write out the resulting files.
         * zDrives is DrivesEnabled normally, or DrivesAll when it 
         * is called from an RaidSixDriveImporter (in the process to become "enabled")
         */
        for (int disk = 0; disk < total_shards; disk++) {

        	if (isWrite(disk)) {
	        	String dirPath = getDrives().get(disk).getBucketObjectDataDirPath(bucketName)      + ((o_version.isEmpty()) ? "" : (File.separator + VirtualFileSystemService.VERSION_DIR));
	        	String name = objectName+ "." + String.valueOf(chunk) +"." + String.valueOf(disk)  + (o_version.isEmpty()   ? "" : "v." +  String.valueOf(o_version.get().intValue()));
	        	
	        	File outputFile = new File(dirPath, name);
				try  (OutputStream out = new BufferedOutputStream(new FileOutputStream(outputFile))) {
					out.write(shards[disk]);
		        } catch (FileNotFoundException e) {
					throw new InternalCriticalException(e, "b:" +bucketName + ", o:" + objectName +" f: " + name);
				} catch (IOException e) {
					throw new InternalCriticalException(e, "b:" +bucketName + ", o:" + objectName +" f: " + name);
				}
				this.encodedInfo.encodedBlocks.add(outputFile);
        	}
        }
		return eof;
    }
    
    
	protected boolean isWrite(int disk) {
		return true;
	}

	public RAIDSixDriver getDriver() {
		return this.driver;
	}

	public VirtualFileSystemService getVFS() {
		return this.driver.getVFS();
	}
	
    protected List<Drive> getDrives() {
    	return zDrives;
    }

	
}
