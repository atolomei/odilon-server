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

package io.odilon.vfs.raid6;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.model.ServerConstant;
import io.odilon.util.Check;
import io.odilon.vfs.model.BucketIterator;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.ServerBucket;

/**
 * <p><b>RAID 6 Bucket iterator</b> <br/>
 * Data is partitioned into blocks encoded using RS Erasure code and stored on all drives.<br/> 
 * The encoding convention for blocks in File System is detailed in {@link RAIDSixDriver}</p>
 *
 * <p>This {@link BucketIterator} uses a randomly selected {@link Drive} to iterate and 
 * return {@link ObjectMetata}instances. All Drives contain all {@link ObjectMetadata} in RAID 6.</p>
 * 
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public class RAIDSixBucketIterator extends BucketIterator implements Closeable {

	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(RAIDSixBucketIterator.class.getName());
	
	@JsonProperty("drive")
	private final Drive drive;
	
	@JsonProperty("cumulativeIndex")		
	private long cumulativeIndex = 0;

	
	@JsonIgnore
	private Iterator<Path> it;
	
	@JsonIgnore
	private Stream<Path> stream;
	
	//@JsonIgnore
	//RAIDSixDriver driver;

	/**
	 * 
	 * @param driver		can not be null
	 * @param bucketName  	can not be null
	 * @param opOffset
	 * @param opPrefix
	 */
	public RAIDSixBucketIterator(RAIDSixDriver driver, ServerBucket bucket, Optional<Long> opOffset,  Optional<String> opPrefix) {
		super(driver, bucket);
		
		opPrefix.ifPresent( x -> setPrefix(x.toLowerCase().trim()));
		opOffset.ifPresent( x -> setOffset(x));
		
		/** must use DrivesEnabled */
		this.drive  = driver.getDrivesEnabled().get(Double.valueOf(Math.abs(Math.random()*1000)).intValue() % getDriver().getDrivesEnabled().size());
	}
	
	
	/**
	@Override
	public boolean hasNext() {
		
		if(!isInitiated()) {
			init();
			return fetch();
		}
		
		if (getRelativeIndex() < getBuffer().size())
			return true;
				
		return fetch();
	}**/

	/**
	@Override
	public Path next() {
		
		if (getRelativeIndex() < getBuffer().size()) {
			Path object = getBuffer().get(getRelativeIndex());
			setRelativeIndex(getRelativeIndex()+1); 
			this.cumulativeIndex++; 
			return object;
		}

		boolean hasItems = fetch();
		
		if (!hasItems)
			throw new IndexOutOfBoundsException( "No more items available. hasNext() should be called before this method. "
												 + "[returned so far -> " + String.valueOf(cumulativeIndex)+"]" );
		
		Path object = getBuffer().get(getRelativeIndex());

		incRelativeIndex();
		this.cumulativeIndex++;
		
		return object;

	}
	*/
	
	@Override
	public synchronized void close() throws IOException {
		if (this.stream!=null)
			this.stream.close();
	}
	
	@Override
	protected void init() {
		
		Path start = new File(getDrive().getBucketMetadataDirPath(getBucketId())).toPath();
		try {
			this.stream = Files.walk(start, 1).skip(1).
					filter(file -> Files.isDirectory(file)).
					filter(file -> (getPrefix()==null) || file.getFileName().toString().toLowerCase().startsWith(getPrefix()));
					//filter(file -> isObjectStateEnabled(file));
			
		} catch (IOException e) {
			throw new InternalCriticalException(e, "Files.walk ...");
		}
		this.it = this.stream.iterator();
		skipOffset();
		setInitiated(true);
	}
	
	

	/**
	 * @return false if there are no more items
	 */
	@Override
	protected boolean fetch() {

		setRelativeIndex(0);
		
		setBuffer(new ArrayList<Path>());
		boolean isItems = true;
		while (isItems && getBuffer().size() < ServerConstant.BUCKET_ITERATOR_DEFAULT_BUFFER_SIZE) {
			if (it.hasNext())
				getBuffer().add(it.next());		
			else 
				isItems = false;
		}
		return !getBuffer().isEmpty();
	}

	 

	private Drive getDrive() {
		return this.drive;
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


	
}
