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
package io.odilon.virtualFileSystem.raid1;

import java.io.File;

import java.io.IOException;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * Rollback is the same for both operations <br/>
 * <ul>
 * <li>{@link OperationCode.DELETE_OBJECT}</li>
 * <li>{@link OperationCode.DELETE_OBJECT_PREVIOUS_VERSIONS}</li>
 * </ul>
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDOneRollbackDeleteHandler extends RAIDOneRollbackHandler {

	private static Logger logger = Logger.getLogger(RAIDOneRollbackDeleteHandler.class.getName());

	public RAIDOneRollbackDeleteHandler(RAIDOneDriver driver, VirtualFileSystemOperation operation, boolean recovery) {
		super(driver, operation, recovery);
	}

	@Override
	protected void rollback() {

		boolean rollbackOK = false;

		try {
			rollbackOK = restore();
		} catch (InternalCriticalException e) {
			if (!isRecovery())
				throw (e);
			else
				logger.error(opInfo(getOperation()), SharedConstant.NOT_THROWN);
		} catch (Exception e) {
			if (!isRecovery())
				throw new InternalCriticalException(e, opInfo(getOperation()));
			else
				logger.error(opInfo(getOperation()), SharedConstant.NOT_THROWN);
		} finally {
			if (rollbackOK || isRecovery())
				getOperation().cancel();
		}
	}

	private boolean restore() {
		ServerBucket bucket = getCacheBucket(getOperation().getBucketId());
		/** restore metadata directory */
		for (Drive drive : getDriver().getDrivesAll()) {
			String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucket) + File.separator + getOperation().getObjectName();
			String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucket, getOperation().getObjectName());
			try {
				FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
			} catch (IOException e) {
				throw new InternalCriticalException(e, objectInfo(bucket, getOperation().getObjectName()));
			}
		}
		return true;
	}
}
