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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
 
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.util.DateTimeUtil;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * <p>
 * ErasureCoding. Sync Object. This class regenerates the object's chunks when a new
 * disk is added
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class ECSyncObjectHandler extends ECTransactionHandler {

	private static Logger logger = Logger.getLogger(ECSyncObjectHandler.class.getName());

	@JsonIgnore
	private List<Drive> drives;

	@JsonIgnore
	private List<Drive> drivesToSync;

	/**
	 * Metadata of the object currently being synced.
	 * Set as the first statement of {@link #sync} so that {@link #getDrives()} and
	 * {@link #getDrivesToSync()} can scope their results to the correct volume.
	 */
	@JsonIgnore
	private ObjectMetadata syncMeta;

	/**
	 * @param driver can not be null
	 */
	protected ECSyncObjectHandler(ECDriver driver) {
		super(driver);
	}

	/**
	 * @param meta can not be null
	 */
	public void sync(ObjectMetadata meta) {

		// Must be set before getDrives() / getDrivesToSync() are first called.
		this.syncMeta = meta;

		VirtualFileSystemOperation operation = null;
		boolean done = false;
		ServerBucket bucket;

		objectWriteLock(meta.getBucketId(), meta.getObjectName());
		try {

			bucketReadLock(meta.getBucketId());
			try {

				/** must be executed inside the critical zone. */
				checkExistsBucket(meta.getBucketId());

				bucket = getBucketCache().get(meta.getBucketId());

		 		// If this object's volume has no NOTSYNC drives there is nothing to
				// re-encode. This is the normal case when a brand-new volume is added:
				// existing objects live on an older volume whose drives are all ENABLED;
				// the NOTSYNC drives belong to the new, still-empty volume.
				if (getDrivesToSync().isEmpty())
					return;
 
				/**
				 * backup metadata, there is no need to backup data because existing data files
				 * are not touched.
				 **/
				backup(bucket, meta);
				operation = getJournalService().syncObject(bucket, meta.getObjectName());
				syncHead(meta, bucket);
				syncVersions(meta, bucket);
				done = operation.commit();

			} finally {
				try {
					if ((!done)) {
						try {
							rollback(operation);
						} catch (Exception e) {
							throw new InternalCriticalException(e, objectInfo(meta));
						}
					}
				} finally {
					bucketReadUnLock(meta.getBucketId());
				}
			}
		} finally {
			objectWriteUnLock(meta.getBucketId(), meta.getObjectName());
		}
	}

	/**
	 * <p>
	 * Returns the ordered list of drives used for encoding.
	 * </p>
	 * <p>
	 * <b>Multi-volume fix (Bug 1):</b> when {@link #syncMeta} is set the list is
	 * scoped to the <em>object's own volume</em>. The encoder loop runs
	 * {@code 0 .. total_shards-1} using volume-local indices. Returning
	 * {@code drivesAll} (which spans all volumes) places NOTSYNC drives at indices
	 * &ge; {@code total_shards} so they are never written. Using the volume-local
	 * list ensures each index maps to the correct drive.
	 * </p>
	 */
	protected synchronized List<Drive> getDrives() {

		if (this.drives != null)
			return this.drives;

		if (this.syncMeta != null) {
			// Volume-aware path: use only the drives belonging to this object's volume.
			this.drives = new ArrayList<>(getDriver().getVolumeForObject(this.syncMeta).getDrives());
		} else {
			// Fallback: single-volume deployment or syncMeta not yet set.
			this.drives = new ArrayList<Drive>();
			getDriver().getDrivesAll().forEach(d -> this.drives.add(d));
			this.drives.sort(new Comparator<Drive>() {
				@Override
				public int compare(Drive o1, Drive o2) {
					try {
						return o1.getDriveInfo().getOrder() < o2.getDriveInfo().getOrder() ? -1 : 1;
					} catch (Exception e) {
						return 0;
					}
				}
			});
		}
		return this.drives;
	}

	protected synchronized List<Drive> getDrivesToSync() {
		if (this.drivesToSync != null)
			return this.drivesToSync;
		this.drivesToSync = new ArrayList<Drive>();
		getDrives().forEach(d -> {
			if (d.getDriveInfo().getStatus() == DriveStatus.NOTSYNC)
				this.drivesToSync.add(d);
		});

		return this.drivesToSync;
	}

	/**
	 * <p>
	 * copy metadata directory <br/>
	 * . back up the full metadata directory (ie. ObjectMetadata for all versions)
	 * </p>
	 * 
	 * @param bucket
	 * @param objectName
	 */
	private void backup(ServerBucket bucket, ObjectMetadata meta) {
		try {
			for (Drive drive : getDriver().getDrivesEnabled()) {
				File src = new File(drive.getObjectMetadataDirPath(bucket, meta.getObjectName()));
				File dest = new File(drive.getBucketWorkDirPath(bucket) + File.separator + meta.getObjectName());
				if (src.exists())
					FileUtils.copyDirectory(src, dest);
			}

		} catch (IOException e) {
			throw new InternalCriticalException(e, objectInfo(meta));
		}
	}

	private void syncHead(ObjectMetadata meta, ServerBucket bucket) {

		{
			/** Data (head) */
			ECDecoder decoder = new ECDecoder(getDriver());
			File file = decoder.decodeHead(meta, bucket);

			ECDriveSyncEncoder driveInitEncoder = new ECDriveSyncEncoder(getDriver(), getDrives());

			try (InputStream in = new BufferedInputStream(new FileInputStream(file.getAbsolutePath()))) {
			
				@SuppressWarnings("unused")
				ECShards shards = driveInitEncoder.encodeHead(in, bucket, meta.getObjectName());
			
				/** MetaData (head) */
				meta.setDateSynced(DateTimeUtil.now());
				logger.debug("Synced -> b:" + meta.getBucketName()+ " o:" + meta.getObjectName() + "  sha 256:" +  meta.getSha256()  );

				
			} catch (FileNotFoundException e) {
				throw new InternalCriticalException(e, objectInfo(meta));
			} catch (IOException e) {
				throw new InternalCriticalException(e, objectInfo(meta));
			}
			
			
			List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
			getDrivesToSync().forEach(d -> list.add(meta));
			saveRAIDSixObjectMetadataToDisk(getDrivesToSync(), list, true);

			logger.debug("Synced -> " + objectInfo(meta) + "  head version:" + String.valueOf(meta.getVersion()));
		
		}
	}

	private void syncVersions(ObjectMetadata meta, ServerBucket bucket) {

		
		// The correct guard is meta.getVersion() > 0.  The version field in the head
		// metadata is the source of truth for how many previous-version files were
		// written to disk.  The VersionControl setting governs whether NEW updates
		// create new versions; it must NOT suppress replication of already-existing
		// versions to a replacement drive.
		//
		// If a version file does not actually exist on the source drives
		// (e.g. it was cleaned up while VC was disabled), getObjectMetadataVersion
		// returns null and the inner block is skipped with a warn log — safe.
		if (meta.getVersion() > 0) {

			for (int version = 0; version < meta.getVersion(); version++) {

				// NOTSYNC drive could be chosen as the metadata source.
				// The original code picked a random drive from the full volume drive list,
				// which includes the NOTSYNC replacement drive.  That drive has no version
				// metadata yet; picking it returns null → the version is skipped silently.
				// Restrict the pool to ENABLED drives only so the source is always valid.
				List<Drive> vDrives = getDriver().getVolumeForObject(meta).getDrives();
				List<Drive> enabledVDrives = vDrives.stream()
						.filter(d -> d.getDriveInfo().getStatus() == DriveStatus.ENABLED)
						.collect(Collectors.toList());
				List<Drive> readPool = enabledVDrives.isEmpty() ? vDrives : enabledVDrives;
				Drive vReadDrive = readPool.get(Double.valueOf(Math.abs(Math.random() * 1000)).intValue() % readPool.size());
				ObjectMetadata versionMeta = vReadDrive.getObjectMetadataVersion(bucket, meta.getObjectName(), version);

				if (versionMeta != null) {

					/** Data (version) */
					ECDecoder decoder = new ECDecoder(getDriver());
					ECDriveSyncEncoder driveEncoder = new ECDriveSyncEncoder(getDriver(), getDrives());
					
					
					try (InputStream in = new BufferedInputStream(new FileInputStream(decoder.decodeVersion(versionMeta, bucket).getAbsolutePath()))) {
						/**
						 * encodes version without saving existing blocks, only the ones that go to the
						 * new drive/s
						 */
						ECShards shards = driveEncoder.encodeVersion(in, bucket, meta.getObjectName(), versionMeta.getVersion());
						
					 
						
						/** Metadata (version) */
						/**
						 * changes the date of sync in order to prevent this object's sync if the
						 * process is re run
						 */
						versionMeta.setDateSynced(DateTimeUtil.now());
						meta.setSha256( shards.getSrcSha256());
						

						
					} catch (FileNotFoundException e) {
						throw new InternalCriticalException(e, objectInfo(meta));
					} catch (IOException e) {
						throw new InternalCriticalException(e, objectInfo(meta));
					}

				// syncHead() already scopes its save to getDrivesToSync(). Without this fix
				// version metadata is written to all drives (getDrives()), overwriting
				// existing metadata on ENABLED drives and causing a list/drives size mismatch
				// when multiple volumes are present.
				List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();
				getDrivesToSync().forEach(d -> list.add(versionMeta));
				saveRAIDSixObjectMetadataToDisk(getDrivesToSync(), list, false);

				} else {
					logger.warn("previous version was deleted for Object -> " + String.valueOf(version) + " |  head " + objectInfo(meta) + "  head version:" + String.valueOf(meta.getVersion()));
				}
			}
		}
	}

}
