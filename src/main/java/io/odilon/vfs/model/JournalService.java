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
package io.odilon.vfs.model;




import io.odilon.service.SystemService;

/**
 * <p>Service for ensuring Data Integrity<br/>
 * Changes to data files must be written only after those changes have been logged, that is, <br/> 
 * after log records describing the changes have been flushed to permanent storage. <br/>  
 * This is roll-forward recovery, also known as REDO.</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public interface JournalService extends SystemService {
	

	/** -----------------
	 * KEY
	 * ------------------*/
	public VFSOperation saveServerKey();

	
	
	/** -----------------
	 * SERVER
	 * ------------------*/
	public VFSOperation createServerMetadata();
	public VFSOperation updateServerMetadata();
	

	/** -----------------
	 * BUCKET
	 * ------------------*/

	public VFSOperation createBucket(Long bucketId, String bucketName);
	public VFSOperation updateBucket(ServerBucket bucket);
	public VFSOperation deleteBucket(ServerBucket bucket);
	
	/** -----------------
	 * OJBECT
	 * ------------------*/		
	public VFSOperation createObject(ServerBucket bucket, String objectName);
	public VFSOperation updateObject(ServerBucket bucket, String objectName, int version);
	
	public VFSOperation updateObjectMetadata(ServerBucket bucket, String objectName, int version);

	/** Version control */
	public VFSOperation restoreObjectPreviousVersion(Long bucketId, String objectName, int versionToRestore);
	public VFSOperation deleteObject(Long bucketId, String objectName, int currentHeadVersion);
	public VFSOperation deleteObjectPreviousVersions(Long bucketId, String objectName, int currentHeadVersion);
	

	/** sync new drive */
	public VFSOperation syncObject(Long bucketId, String objectName);
	
	
	
	/** ----------------- */
	
	public boolean commit(VFSOperation opx);
	public boolean cancel(VFSOperation opx);
	
	
	public String newOperationId();
	
	
	
	

	
	

}
