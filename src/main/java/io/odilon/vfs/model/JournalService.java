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

	public VFSOperation createBucket(String bucketName);
	public VFSOperation deleteBucket(String bucketName);
	
	/** -----------------
	 * OJBECT
	 * ------------------*/		
	public VFSOperation createObject(String bucketName, String objectName);
	public VFSOperation updateObject(String bucketName, String objectName, int version);
	
	public VFSOperation updateObjectMetadata(String bucketName, String objectName, int version);

	/** Version control */
	public VFSOperation restoreObjectPreviousVersion(String bucketName, String objectName, int versionToRestore);
	public VFSOperation deleteObject(String bucketName, String objectName, int currentHeadVersion);
	public VFSOperation deleteObjectPreviousVersions(String bucketName, String objectName, int currentHeadVersion);
	

	/** sync new drive */
	public VFSOperation syncObject(String bucketName, String objectName);
	
	
	
	/** ----------------- */
	
	public boolean commit(VFSOperation opx);
	public boolean cancel(VFSOperation opx);
	
	
	public String newOperationId();
	
	
	
	

	
	

}
