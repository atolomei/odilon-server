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

import java.io.InputStream;
import java.util.List;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveStatus;

/**<p>Encoder for Drive Sync process</p>
 * 
 * <p>The difference between {@code RSDriveInitializationEncoder} and the standard {@link RAIDSixEncoder} 
 * is that this one only saves the RS Blocks that go to the newly added disks (ie. blocks on enabled 
 * Drives are <b>not</b> touched).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDSixSDriveSyncEncoder extends RAIDSixEncoder {
		
	@SuppressWarnings("unused")	
	static private Logger logger = Logger.getLogger(RAIDSixSDriveSyncEncoder.class.getName());

	
	/**
	 * 
	 * @param driver can not be null
	 */
	protected RAIDSixSDriveSyncEncoder(RAIDSixDriver driver) {
    	super(driver);
    }
	protected RAIDSixSDriveSyncEncoder(RAIDSixDriver driver, List<Drive> zDrives) {
		super(driver, zDrives);
	}

	/**
	 * <p> We can not use the {@link ObjectMetadata} here because it may 
	 * not exist yet. The steps to upload objects are: <br/>
	 * - upload binary data <br/>
	 * - create ObjectMetadata <br/>
	 * </p>
 	 */
	public RAIDSixBlocks encodeHead (InputStream is, Long bucketId, String objectName) {
		return super.encodeHead(is, bucketId, objectName);
	}

	/**
	 * <p> We can not use the {@link ObjectMetadata} here because it may 
	 * not exist yet. The steps to upload objects are: <br/>
	 * - upload binary data <br/>
	 * - create ObjectMetadata <br/>
	 * </p>
 	 */

	public RAIDSixBlocks encodeVersion (InputStream is, Long bucketId, String objectName, int version) {
		return super.encodeVersion(is, bucketId, objectName, version);
	}
	
	/**
	 * <p>only write on Drives in status {@link DriveStatus.NOTSYNC}</p>
	 */
	protected boolean isWrite(int disk) {
		return (getDrives().get(disk).getDriveInfo().getStatus()==DriveStatus.NOTSYNC);
	}
	
	
}
