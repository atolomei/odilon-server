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
package io.odilon.vfs.raid0;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ObjectStatus;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.ServerBucket;
import io.odilon.vfs.model.BucketIterator;

/**
 * <p>RAID 0. Bucket Iterator <br/>
 *  All Drives are enabled in RAID 0 because the Drive sync process is blocking 
 *  when the {@link VirtualFileSystemService} starts
 *  </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDZeroBucketIterator extends BucketIterator implements Closeable {
			
	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(RAIDZeroBucketIterator.class.getName());
	
	@JsonProperty("prefix")
	private String prefix = null;
	
	@JsonProperty("cumulativeIndex")
	private long cumulativeIndex = 0;
	
	@JsonIgnore
	private Map<Drive, Iterator<Path>> itMap;
	
	@JsonIgnore
	private Map<Drive, Stream<Path>> streamMap;
	
	@JsonIgnore
	private List<Path> buffer;
	
	/** next item to return -> 0 to (buffer.size() - 1) */
	@JsonIgnore
	private int relativeIndex = 0;  
	
	@JsonIgnore
	private List<Drive> drives;
	
	@JsonIgnore
	RAIDZeroDriver driver;
	
	@JsonIgnore
	private boolean initiated = false;


	public RAIDZeroBucketIterator(RAIDZeroDriver driver, ServerBucket bucket, Optional<Long> opOffset,  Optional<String> opPrefix) {
			this(driver, bucket, opOffset, opPrefix, Optional.empty());
	}

	public RAIDZeroBucketIterator(RAIDZeroDriver driver, ServerBucket bucket, Optional<Long> opOffset,  Optional<String> opPrefix, Optional<String> serverAgentId) {
			super(bucket);

		opOffset.ifPresent(x -> setOffset(x));
		serverAgentId.ifPresent( x -> setAgentId(x));
		opPrefix.ifPresent(x -> this.prefix=x);
		this.driver = driver;
		
		/** after the VirtualFileService starts up
		 * all drives are in state {@link DriveStatus.ENABLED} in RAID 0 */
		this.drives = new ArrayList<Drive>();
		this.drives.addAll(driver.getDrivesEnabled());
		
		this.streamMap = new HashMap<Drive, Stream<Path>>();
		this.itMap = new HashMap<Drive, Iterator<Path>>();
	}
	
	@Override
	public synchronized boolean hasNext() {
		
		if(!this.initiated) {
			init();
			return fetch();
		}
		/** if the buffer still has items */
		if (this.relativeIndex < this.buffer.size())
			return true;
				
		return fetch();
	}
	
	/**
	 * 
	 */
	@Override
	public synchronized Path next() {
		
		/** if the buffer still has items to return  */
		if (this.relativeIndex < this.buffer.size()) {
			Path object = this.buffer.get(this.relativeIndex);
			this.relativeIndex++; 
			this.cumulativeIndex++; 
			return object;
		}

		boolean hasItems = fetch();
		
		if (!hasItems)
			throw new IndexOutOfBoundsException("No more items available [returned so far -> " + String.valueOf(cumulativeIndex)+"]");
		
		Path object = this.buffer.get(this.relativeIndex);

		this.relativeIndex++;
		this.cumulativeIndex++;
		
		return object;
	}

	@Override
	public synchronized void close() throws IOException {
		if (this.streamMap==null)
			return;
		this.streamMap.forEach((k,v) -> v.close());
	}
	
	/**
	 * <p>There are no Drives in mode {@link DriveStatus#NOTSYNC} in RAID 0. <br/> 
	 * All new drives are synced before the VirtualFileSystemService 
	 * completes its initialization. </p>
	 *  
	 * @return
	 */
	protected List<Drive> getDrives() {
		return this.drives;
	}

	/**
	 * 
	 */
	private void init() {
		for (Drive drive: getDrives()) {
			Path start = new File(drive.getBucketMetadataDirPath(getBucketId())).toPath();
			Stream<Path> stream = null;
			try {
				stream = Files.walk(start, 1).
						skip(1).
						filter(file -> Files.isDirectory(file)).
						filter(file -> (prefix==null) || (file.getFileName().toString().toLowerCase().startsWith(prefix)));
						//filter(file -> isObjectStateEnabled(file));
				this.streamMap.put(drive, stream);		
			} catch (IOException e) {
				throw new InternalCriticalException(e, "Files.walk ...");
			}
			Iterator<Path> it = stream.iterator();
			this.itMap.put(drive, it);
		}
		skipOffset();
		this.initiated = true;
	}

	@SuppressWarnings("unused")
	private boolean isObjectStateEnabled(Path path) {
		ObjectMetadata meta = driver.getObjectMetadata(getBucket(), path.toFile().getName());
		if (meta==null)
			return false;
		if (meta.status == ObjectStatus.ENABLED) 
			return true;
		return false;
	}
	
	/**
	 * 
	 */
	private void skipOffset() {

		if (getOffset()==0)
				return;

			boolean isItems = false;
			{
				for (Drive drive: this.getDrives()) {
					if (this.itMap.get(drive).hasNext()) {
						isItems = true;
						break;
					}
				}
			}
			long skipped = this.cumulativeIndex;
			
			while (isItems && skipped<getOffset()) {
				int d_index = 0;
				int d_poll = d_index++ % this.getDrives().size();
				Drive drive = this.getDrives().get(d_poll);
				Iterator<Path> iterator = itMap.get(drive);
				if (iterator.hasNext()) {
					iterator.next();
					skipped++;
				}
				else {
					/** drive has no more items */
					this.streamMap.get(drive).close();
					this.itMap.remove(drive);
					this.getDrives().remove(d_poll);
					isItems = !this.getDrives().isEmpty();
				}
			}
	}
	
	/**
	 * @return
	 */
	private boolean fetch() {

		this.relativeIndex = 0;
		this.buffer = new ArrayList<Path>();
		
		boolean isItems = false;

		if (this.getDrives().isEmpty())
			return false;
		
		{
			for (Drive drive: this.getDrives()) {
				if (this.itMap.get(drive).hasNext()) {
					isItems = true;
					break;
				}
			}
		}
		{
			int buffer_index = 0;
			while (isItems && (this.buffer.size() < ServerConstant.BUCKET_ITERATOR_DEFAULT_BUFFER_SIZE)) {
				int dPoll = buffer_index++ % this.getDrives().size();
				Drive drive = this.getDrives().get(dPoll);
				Iterator<Path> iterator = this.itMap.get(drive);
				if (iterator.hasNext()) {
					this.buffer.add(iterator.next());		
				}
				else {
					/** drive has no more items */
					this.streamMap.get(drive).close();
					this.itMap.remove(drive);
					this.getDrives().remove(dPoll);
					isItems = !this.getDrives().isEmpty();
				}
			}
		}
		return  ( !this.buffer.isEmpty() );
	}
}
