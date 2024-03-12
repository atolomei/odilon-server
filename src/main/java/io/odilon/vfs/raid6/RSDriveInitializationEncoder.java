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
import java.util.Optional;

import io.odilon.log.Logger;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.DriveStatus;

/**
 * <p>The difference between this encoder and the standard {@link RSEncoder} 
 * is that this one only saves the RS Blocks that go to the newly added disks 
 * (ie. blocks on enabled Drives are not touched).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RSDriveInitializationEncoder extends RSEncoder {
		
	@SuppressWarnings("unused")	
	static private Logger logger = Logger.getLogger(RSDriveInitializationEncoder.class.getName());

	protected RSDriveInitializationEncoder(RAIDSixDriver driver) {
    	super(driver);
    }
	protected RSDriveInitializationEncoder(RAIDSixDriver driver, List<Drive> zDrives) {
		super(driver, zDrives);
	}
	
	public RSFileBlocks encode (InputStream is, String bucketName, String objectName) {
		return super.encode(is, bucketName, objectName);
	}
	
	public RSFileBlocks encode (InputStream is, String bucketName, String objectName, Optional<Integer> version) {
		return super.encode(is, bucketName, objectName, version);
	}
	
	protected boolean isWrite(int disk) {
		return (getDrives().get(disk).getDriveInfo().getStatus()==DriveStatus.NOTSYNC);
	}
	
	
}
