/*
 * Odilon Object Storage
 * (c) kbee 
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
package io.odilon.virtualFileSystem;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.odilon.log.Logger;
import io.odilon.model.RedundancyLevel;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.SimpleDrive;

/**
 * <p>
 * Used by: <br/>
 * RAID 0 {@link RAIDZeroDriver} <br/>
 * RAID 1 {@link RAIDOneDriver}. <br/>
 * <br/>
 * This class is not used by RAID 6 {@link ECDriver}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */

@JsonInclude(Include.NON_NULL)
@NotThreadSafe
@Component
@Scope("prototype")
public class OdilonRaidZeroRaidOneDrive extends OdilonDrive implements SimpleDrive {

	static private Logger logger = Logger.getLogger(OdilonRaidZeroRaidOneDrive.class.getName());

	/**
	 * Constructor called by Spring.io
	 * 
	 * @param rootDir
	 */
	@Autowired
	protected OdilonRaidZeroRaidOneDrive(String rootDir) {
		super(rootDir);
	}

	/**
	 * <p>
	 * Constructor explicit
	 * 
	 * 
	 * @param driveNanme
	 * @param rootDir
	 */
	protected OdilonRaidZeroRaidOneDrive(String driveName, String rootDir, int configOrder, String raidSetup, int raidDrives) {
		super(driveName, rootDir, configOrder, raidSetup, raidDrives);
		
		Check.requireTrue(	raidSetup.equals(RedundancyLevel.RAID_0.getName()) || 
							raidSetup.equals(RedundancyLevel.RAID_1.getName()) ,"raidSetup must be " + RedundancyLevel.RAID_0.getName() + " or " + RedundancyLevel.RAID_1.getName() + " and it is -> " + raidSetup);

		
	}

	/**
	 * Dir -> directory Path -> to File File -> File
	 * 
	 */
	@Override
	public File putObjectStream(Long bucketId, String objectName, InputStream stream) throws IOException {

		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null -> b:" + bucketId.toString());

		/** data Bucket created on demand */
		createDataBucketDirIfNotExists(bucketId);

		try {
			ObjectPath path = new ObjectPath(this, bucketId, objectName);
			String dataFilePath = path.dataFilePath().toString();
			transferTo(stream, dataFilePath);
			return new File(dataFilePath);
		} catch (IOException e) {
			logger.error(e.getClass().getName() + " putObjectStream -> " + "b:" + bucketId.toString() + ", o:" + objectName + ", d:" + getName());
			throw (e);
		}
	}

	@Override
	public void putObjectDataFile(Long bucketId, String objectName, File objectFile) throws IOException {
		Check.requireNonNullArgument(bucketId, "bucketId is null");
		try (InputStream is = new FileInputStream(objectFile)) {
			putObjectStream(bucketId, objectName, is);
		}
	}

	@Override
	public void putObjectDataVersionFile(Long bucketId, String objectName, int version, File objectFile) throws IOException {
		Check.requireNonNullArgument(bucketId, "bucketId is null");
		try (InputStream is = new BufferedInputStream(new FileInputStream(objectFile))) {
			putObjectDataVersionStream(bucketId, objectName, version, is);
		}
	}

	protected File putObjectDataVersionStream(Long bucketId, String objectName, int version, InputStream stream) throws IOException {
		Check.requireNonNullArgument(bucketId, "bucketId is null");
		Check.requireNonNullStringArgument(objectName, "objectName can not be null -> b:" + bucketId.toString());

		try {
			ObjectPath path = new ObjectPath(this, bucketId, objectName);
			String dataFilePath = path.dataFileVersionPath(version).toString();
			transferTo(stream, dataFilePath);
			return new File(dataFilePath);
		} catch (IOException e) {
			logger.error(e.getClass().getName() + " -> " + "b:" + bucketId.toString() + ", o:" + objectName + ", d:" + getName());
			throw (e);
		}
	}

}
