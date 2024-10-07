/*
 * Odilon Object Storage
 * (C) Novamens 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.odilon.vfs.raid1;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.BucketIterator;

/**
 * <p><b>RAID 1</b> <br/>
 * Data is replicated and all Drives have all files</p>
 * Drives that are in status NOT_SYNC are being synced un background. 
 * All new Objects (since the Drive started the sync process) are replicated in the NOT_SYNC drive  while the drive is being synced.
 *
 *<p>The RAID 1 {@link BucketIterator} uses a randomly 
 *   selected Drive (among the drives in status DriveStatus.ENABLED) to walk and return ObjectMetaata files</p>
 *   
 *  @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDOneBucketIterator extends BucketIterator implements Closeable {
		
	private static final Logger logger = Logger.getLogger(RAIDOneBucketIterator.class.getName());
	
	@JsonProperty("prefix")
	private String prefix;
	
	@JsonProperty("drive")
	private final Drive drive;
	
	@JsonProperty("cumulativeIndex")		
	private long cumulativeIndex = 0;
	
	/** next item to return -> 0 .. [ list.size()-1 ] */
	@JsonIgnore
	private int relativeIndex = 0;  
	
	@JsonIgnore
	private Iterator<Path> it;
	
	@JsonIgnore
	private Stream<Path> stream;
	
	@JsonIgnore
	private List<Path> buffer;
	
	@JsonIgnore
	private boolean initiated = false;

	@JsonIgnore
	RAIDOneDriver driver;
	
	public RAIDOneBucketIterator(RAIDOneDriver driver, ServerBucket bucket, Optional<Long> opOffset,  Optional<String> opPrefix) {
		super(bucket);
		opPrefix.ifPresent( x -> this.prefix = x.toLowerCase().trim());
		opOffset.ifPresent( x -> setOffset(x));
		this.driver = driver;
		this.drive  = driver.getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random()*10000)).intValue() % driver.getDrivesEnabled().size());
	}
	
	@Override
	public synchronized boolean hasNext() {
		
		if(!this.initiated) {
			init();
			return fetch();
		}
		/** 
		 * if the buffer 
		 *  still has items  
		 * **/
		if (this.relativeIndex < this.buffer.size())
			return true;
				
		return fetch();
	}
	/**
	 * 
	 */
	@Override
	public synchronized Path next() {
		
		/**	
		 * if the buffer still has items to return 
		 * */
		if (this.relativeIndex < this.buffer.size()) {
			Path object = this.buffer.get(this.relativeIndex);
			this.relativeIndex++; 
			this.cumulativeIndex++; 
			return object;
		}

		boolean hasItems = fetch();
		
		if (!hasItems)																							
			throw new IndexOutOfBoundsException("No more items available. hasNext() should be called before this method. " + "[returned so far -> " + String.valueOf(cumulativeIndex)+"]");
		
		Path object = this.buffer.get(this.relativeIndex);

		this.relativeIndex++;
		this.cumulativeIndex++;
		
		return object;
	}


	/**
	 * 
	 * 
	 */
	@Override
	public synchronized void close() throws IOException {
		if (this.stream!=null)
			this.stream.close();
	}
	

	private Drive getDrive() {
		return this.drive;
	}
	private void init() {
		
			Path start = new File( getDrive().getBucketMetadataDirPath(getBucketId())).toPath();
			try {
				this.stream = Files.walk(start, 1).skip(1).
						filter(file -> Files.isDirectory(file)).
						filter(file -> this.prefix==null || file.getFileName().toString().toLowerCase().startsWith(this.prefix));
						//filter(file -> isObjectStateEnabled(file));
				
			} catch (IOException e) {
				logger.error(e, SharedConstant.NOT_THROWN);
				throw new InternalCriticalException(e);
			}
			this.it = this.stream.iterator();
		skipOffset();
		this.initiated = true;
	}
	
	
	private void skipOffset() {
		if (getOffset()==0)
			return;
		boolean isItems = true;
		int skipped = 0;
		while (isItems && skipped<getOffset()) {
			if (this.it.hasNext()) {
				this.it.next();
				skipped++;
			}
			else {
				break;
			}
		}
	}

	/**
	 * @return false if there are no more items
	 */
	private boolean fetch() {
		this.relativeIndex = 0;
		this.buffer = new ArrayList<Path>();
		boolean isItems = true;
		while (isItems && this.buffer.size() < ServerConstant.BUCKET_ITERATOR_DEFAULT_BUFFER_SIZE) {
			if (it.hasNext())
				this.buffer.add(it.next());		
			else 
				isItems = false;
		}
		return !this.buffer.isEmpty();
	}
	
	private RAIDOneDriver getDriver() {
		return driver;
	}
	
	/**
	 * <p>This method should be used when the delete 
	 *  operation is logical (ObjectStatus.DELETED)</p>
	 * @param path
	 * @return
	 */
	@SuppressWarnings("unused")
	private boolean isObjectStateEnabled(Path path) {
		ObjectMetadata meta = getDriver().getObjectMetadata(getBucket(), path.toFile().getName());
		if (meta==null)
			return false;
		if (meta.status == ObjectStatus.ENABLED) 
			return true;
		return false;
	}

}
	
