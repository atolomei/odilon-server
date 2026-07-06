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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.io.FileUtils;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;

import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
public class ECRollbackUpdateHandler extends ECRollbackHandler {

	private static Logger logger = Logger.getLogger(ECRollbackUpdateHandler.class.getName());

	public ECRollbackUpdateHandler(ECDriver driver, VirtualFileSystemOperation operation, boolean recovery) {
		super(driver, operation, recovery);
	}

	@Override
	protected void rollback() {

		switch (getOperation().getOperationCode()) {
		case UPDATE_OBJECT: {
			rollbackUpdate();
			break;
		}
		case UPDATE_OBJECT_METADATA: {
			rollbackUpdateMetadata();
			break;
		}
		case RESTORE_OBJECT_PREVIOUS_VERSION: {
			rollbackUpdate();
			break;
		}
		default: {
			throw new IllegalArgumentException(" not supported -> " + opInfo(getOperation()));
		}
		}
	}

	private void rollbackUpdateMetadata() {

		boolean done = false;
		try {
			restoreVersionObjectMetadata();
			done = true;
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
			if (done || isRecovery()) {
				getOperation().cancel();
			}
		}
	}

	private void rollbackUpdate() {
		boolean done = false;
		try {
			restoreVersionObjectDataFile();
			restoreVersionObjectMetadata();
			done = true;

		} catch (InternalCriticalException e) {
			if (!isRecovery())
				throw (e);
			else
				logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);

		} catch (Exception e) {
			if (!isRecovery()) {
				logger.error(e, SharedConstant.THROWN_WRAPPED);
				throw new InternalCriticalException(e, opInfo(getOperation()));
			}
			else
				logger.error(e, opInfo(getOperation()), SharedConstant.NOT_THROWN);
		} finally {
			if (done || isRecovery()) {
				// ── Bug U4 fix: purge stale head artifacts from non-owning volumes ───────────
				// The failed update may have written new head metadata and/or shard files to
				// the active volume BEFORE the journal was committed. After restoring the old
				// version as head on the old volume, getDriverObjectMetadataInternal() searches
				// the active volume first and would return that stale metadata unless we delete
				// it here. Also delete head shard files on non-owning volumes to reclaim space.
				try {
					ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
					purgeStaleHeadFromOtherVolumes(bucket, getOperation().getObjectName(), getOperation().getVersion());
				} catch (Exception e) {
					logger.error(e, "U4 purge stale head | " + opInfo(getOperation()), SharedConstant.NOT_THROWN);
				}
				// ─────────────────────────────────────────────────────────────────────────────
				getOperation().cancel();
			}
		}
	}

	/**
	 * <p>
	 * After a rollback of UPDATE_OBJECT, deletes any head metadata files and head
	 * shard files that the failed update may have written to drives belonging to
	 * volumes <em>other</em> than the one that owns the correctly restored object.
	 * </p>
	 * <p>
	 * <b>Why this is needed (Bug U4):</b> {@code saveObjectMetadata} always writes
	 * to the <em>active</em> volume's drives. When an object lives on an older
	 * (READONLY) volume, a cross-volume update writes new metadata to the active
	 * volume and new shards there too. If the operation is rolled back,
	 * {@link ECDriver#getDriverObjectMetadataInternal} (which searches the
	 * active volume first) will find the stale new-version metadata and serve it
	 * instead of the correctly restored old-version metadata on the original volume.
	 * </p>
	 *
	 * @param bucket     the bucket containing the object
	 * @param objectName the object being rolled back
	 * @param version    the old (pre-update) version that was restored as head;
	 *                   used to read the restored metadata and determine the owning
	 *                   volume
	 */
	private void purgeStaleHeadFromOtherVolumes(ServerBucket bucket, String objectName, int version) {

		// Determine the owning volume from the restored version metadata
		ObjectMetadata restoredMeta = null;
		try {
			restoredMeta = getDriver().getObjectMetadataVersion(bucket, objectName, version);
		} catch (Exception e) {
			// If we can't read it, fall through and attempt a best-effort purge by
			// scanning all non-active volumes.
			logger.error(e, "U4 purge: could not read restored version meta | " + opInfo(getOperation()), SharedConstant.NOT_THROWN);
		}

		int owningVolumeId = (restoredMeta != null) ? restoredMeta.getVolumeId() : -1;

		for (ECVolume vol : getDriver().getVolumeManager().getAllVolumes()) {
			if (vol.getVolumeId() == owningVolumeId)
				continue; // correct volume — leave it intact

			for (Drive drive : vol.getDrives()) {

				// Delete stale head metadata file
				try {
					File metaFile = drive.getObjectMetadataFile(bucket, objectName);
					if (metaFile != null)
						FileUtils.deleteQuietly(metaFile);
				} catch (Exception e) {
					logger.error(e, "U4 purge: metadata delete | drive=" + drive.getName()
							+ " | " + opInfo(getOperation()), SharedConstant.NOT_THROWN);
				}

				// Delete stale head shard files (pattern: objectName.<chunk>.<disk>,
				// no .v suffix — i.e. HEAD files only)
				try {
					File dataDir = new File(drive.getBucketObjectDataDirPath(bucket));
					if (dataDir.exists() && dataDir.isDirectory()) {
						File[] orphans = dataDir.listFiles((dir, name) ->
								name.startsWith(objectName + ".") && !name.contains(".v"));
						if (orphans != null)
							for (File orphan : orphans)
								FileUtils.deleteQuietly(orphan);
					}
				} catch (Exception e) {
					logger.error(e, "U4 purge: shard delete | drive=" + drive.getName()
							+ " | " + opInfo(getOperation()), SharedConstant.NOT_THROWN);
				}
			}
		}
	}

	private boolean restoreVersionObjectMetadata() {
		try {
			boolean success = true;
			ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
			ObjectMetadata versionMeta = getDriver().getObjectMetadataVersion(bucket, getOperation().getObjectName(), getOperation().getVersion());
			// Restore only on the drives of the volume that owns this object
			List<Drive> drives = getDriver().getVolumeForObject(versionMeta).getDrives();
			for (Drive drive : drives) {
				versionMeta.setDrive(drive.getName());
				drive.saveObjectMetadata(versionMeta);
			}
			return success;

		} catch (InternalCriticalException e) {
			throw e;
		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new InternalCriticalException(e, opInfo(getOperation()));
		}
	}

	private boolean restoreVersionObjectDataFile() {
		try {
			ServerBucket bucket = getBucketCache().get(getOperation().getBucketId());
			ObjectMetadata meta = getDriver().getObjectMetadataVersion(bucket, getOperation().getObjectName(), getOperation().getVersion());
			Map<Drive, List<String>> versionToRestore = getDriver().getObjectDataFilesNames(meta, Optional.of(getOperation().getVersion()));
			for (Drive drive : versionToRestore.keySet()) {
				for (String name : versionToRestore.get(drive)) {
					String arr[] = name.split(".v");
					String headFileName = arr[0];
					try {
						if (new File(drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR, name).exists()) {
							Files.copy((new File(drive.getBucketObjectDataDirPath(bucket) + File.separator + VirtualFileSystemService.VERSION_DIR, name)).toPath(), (new File(drive.getBucketObjectDataDirPath(bucket), headFileName)).toPath(),
									StandardCopyOption.REPLACE_EXISTING);
						}
					} catch (IOException e) {
						throw new InternalCriticalException(e, opInfo(getOperation()));
					}
				}
			}
			return true;

		} catch (InternalCriticalException e) {
			throw e;

		} catch (Exception e) {
			logger.error(e, SharedConstant.THROWN_WRAPPED);
			throw new InternalCriticalException(e, opInfo(getOperation()));
		}
	}

}
