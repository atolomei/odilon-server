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
import java.util.List;
import java.util.Optional;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDSixRollbackCreateHandler extends RAIDSixRollbackHandler {

	private static Logger logger = Logger.getLogger(RAIDSixRollbackCreateHandler.class.getName());

	public RAIDSixRollbackCreateHandler(RAIDSixDriver driver, VirtualFileSystemOperation operation, boolean recovery) {
		super(driver, operation, recovery);
	}

	@Override
	protected void rollback() {

		ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());

		boolean done = false;

		try {

			ObjectMetadata meta = null;

			// ── Bug C1 fix: scan ALL drives per volume, not just drive[0] ─────────────────
			//
			// saveRAIDSixObjectMetadataToDisk() writes metadata in parallel to every drive
			// in the active volume. If drive 0 happens to fail its write (partial parallel
			// write) but drives 1..N-1 succeed, the original code's check of only
			// vol.getDrives().get(0) would never find the volume.  In a multi-volume setup
			// the fallback then uses getActiveVolume().getDrives() which may be a DIFFERENT
			// volume (e.g. the admin switched the active volume between the failed CREATE and
			// this journal replay on restart), leaving orphan metadata on the original volume
			// and orphan shard files uncleaned.
			//
			// Fix part 1: check every drive within each volume before moving on.
			// Fix part 2: when meta is still null (shards written, no metadata written at
			//             all before the crash), do a best-effort shard-file cleanup by
			//             scanning every volume's data directory for head shard files
			//             matching the objectName prefix.
			// ─────────────────────────────────────────────────────────────────────────────

			List<Drive> targetDrives = null;
			outer:
			for (RAIDSixVolume vol : getDriver().getVolumeManager().getVolumesInSearchOrder()) {
				for (Drive candidate : vol.getDrives()) {
					File f_meta = candidate.getObjectMetadataFile(bucket, getOperation().getObjectName());
					if (f_meta != null && f_meta.exists()) {
						meta = candidate.getObjectMetadata(bucket, getOperation().getObjectName());
						targetDrives = vol.getDrives();
						break outer;
					}
				}
			}
			if (targetDrives == null)
				targetDrives = getDriver().getActiveVolume().getDrives();

			/** remove metadata dir only on the drives that actually received it */
			for (Drive drive : targetDrives) {
				FileUtils.deleteQuietly(new File(drive.getObjectMetadataDirPath(bucket, getOperation().getObjectName())));
			}

			/** remove data shards */
			if (meta != null) {
				// Normal path: metadata was found so we can derive exact shard file names.
				getDriver().getObjectDataFiles(meta, bucket, Optional.empty()).forEach(file -> {
					FileUtils.deleteQuietly(file);
				});
			} else {
				// Best-effort path (Bug C1 fix part 2): metadata was never written (crash
				// between shard encoding and metadata write). We cannot derive shard file
				// names from metadata, so scan every volume's data directory and delete any
				// head shard file whose name starts with "<objectName>." and has no ".v"
				// version suffix.
				final String prefix = getOperation().getObjectName() + ".";
				for (RAIDSixVolume vol : getDriver().getVolumeManager().getAllVolumes()) {
					for (Drive drive : vol.getDrives()) {
						try {
							java.io.File dataDir = new java.io.File(drive.getBucketObjectDataDirPath(bucket));
							if (dataDir.exists() && dataDir.isDirectory()) {
								java.io.File[] orphans = dataDir.listFiles((dir, name) ->
										name.startsWith(prefix) && !name.contains(".v"));
								if (orphans != null)
									for (java.io.File orphan : orphans)
										FileUtils.deleteQuietly(orphan);
							}
						} catch (Exception e) {
							logger.error(e, "C1 shard scan | drive=" + drive.getName()
									+ " | " + info(), SharedConstant.NOT_THROWN);
						}
					}
				}
			}

			done = true;

		} catch (InternalCriticalException e) {
			if (!isRecovery())
				throw (e);
			else
				logger.error(e, info(), SharedConstant.NOT_THROWN);

		} catch (Exception e) {
			if (!isRecovery())
				throw new InternalCriticalException(e, info());
			else
				logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
		} finally {
			if (done || isRecovery()) {
				getOperation().cancel();
			}
		}
	}

	private String info() {
		return opInfo(getOperation());
	}
}
