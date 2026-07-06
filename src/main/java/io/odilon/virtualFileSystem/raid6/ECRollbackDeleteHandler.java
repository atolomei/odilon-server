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
package io.odilon.virtualFileSystem.raid6;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * 
 * 
 * <p>Erasure Coding. Rollback Delete Object handler</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public class ECRollbackDeleteHandler extends ECRollbackHandler {

	private static Logger logger = Logger.getLogger(ECRollbackDeleteHandler.class.getName());

	public ECRollbackDeleteHandler(ECDriver driver, VirtualFileSystemOperation operation, boolean recovery) {
		super(driver, operation, recovery);
	}

	@Override
	protected void rollback() {

		boolean done = false;
		try {
			restoreMetadata();
			done = true;
		} catch (InternalCriticalException e) {
			if (!isRecovery())
				throw (e);
			else
				logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
		} catch (Exception e) {
			if (!isRecovery()) {
				logger.error(e, SharedConstant.THROWN_WRAPPED);
				throw new InternalCriticalException(e);
			}
			else
				logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
		} finally {
			if (done || isRecovery())
				getOperation().cancel();
		}
	}

	/**
	 * restore metadata directory
	 */
	private void restoreMetadata() {

		ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
		String objectName = getOperation().getObjectName();

		// ── Bug D1 fix: scan ALL drives per volume, not just drive[0] ─────────────────
		// backup() in RAIDSixDeleteObjectHandler writes the work-dir backup to every
		// drive of the owning volume. The original code checked only
		// vol.getDrives().get(0), so if that drive's backup was absent (partial write,
		// drive failure) the volume was never found and drives fell back to
		// getActiveVolume().getDrives() — which in a multi-volume setup may be a
		// completely different volume. This left orphan metadata un-restored and the
		// object permanently gone after rollback.
		// ─────────────────────────────────────────────────────────────────────────────
		List<Drive> drives = null;
		outer:
		for (ECVolume vol : getDriver().getVolumeManager().getVolumesInSearchOrder()) {
			for (Drive d : vol.getDrives()) {
				String backupPath = d.getBucketWorkDirPath(bucket) + File.separator + objectName;
				if (new File(backupPath).exists()) {
					drives = vol.getDrives();
					break outer;
				}
			}
		}
		if (drives == null)
			drives = getDriver().getActiveVolume().getDrives();

		for (Drive drive : drives) {
			String objectMetadataBackupDirPath = drive.getBucketWorkDirPath(bucket) + File.separator + objectName;
			String objectMetadataDirPath = drive.getObjectMetadataDirPath(bucket, objectName);
			try {
				if ((new File(objectMetadataBackupDirPath)).exists())
					FileUtils.copyDirectory(new File(objectMetadataBackupDirPath), new File(objectMetadataDirPath));
			} catch (IOException e) {
				logger.error(e, SharedConstant.THROWN_WRAPPED);
				throw new InternalCriticalException(e, objectInfo(bucket, objectName));
			}
		}
	}

}
