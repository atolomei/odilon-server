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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.BaseRAIDHandler;
import io.odilon.virtualFileSystem.OdilonVirtualFileSystemOperation;
import io.odilon.virtualFileSystem.RAIDHandler;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.IODriver;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
@ThreadSafe
public class ECJournalHandler extends BaseRAIDHandler implements RAIDHandler {

	static private Logger logger = Logger.getLogger(ECDriver.class.getName());
	static private Logger std_logger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private final ECDriver driver;

	public ECJournalHandler(ECDriver driver) {
		this.driver = driver;
	}

	@Override
	public IODriver getDriver() {
		return driver;
	}

	/**
	 * <p>
	 * Erasure Coding multi-volume override: write journal entries only to the
	 * <em>active</em> volume's drives.
	 * </p>
	 * <p>
	 * Rationale: if volume 0 is at OS-level capacity, writing to its drives would
	 * throw an {@link IOException}. Journal files for new operations belong on the
	 * drives that are currently receiving writes. Recovery
	 * ({@link #getJournalPending}) already scans ALL enabled drives, so entries
	 * written here will always be found on restart.
	 * </p>
	 */
	public void saveJournal(VirtualFileSystemOperation op) {
		Check.requireNonNullArgument(op, "operation is null");
		getLockService().getJournalLock().writeLock().lock();
		try {
			for (Drive drive : getRAIDSixDriver().getActiveVolume().getDrives())
				drive.saveJournal(op);
		} finally {
			getLockService().getJournalLock().writeLock().unlock();
		}
	}

	/**
	 * <p>
	 * Erasure Coding multi-volume override: remove journal entries from
	 * <em>all</em> enabled drives across all volumes.
	 * </p>
	 * <p>
	 * On a server restart the active volume may differ from the one that was active
	 * when a pending operation was saved. Scanning all drives ensures stale journal
	 * files left on an old volume's drives are cleaned up.
	 * {@link Drive#removeJournal} uses {@code deleteIfExists} so missing files are
	 * silently ignored — this is correct because {@link #saveJournal} writes only
	 * to the <em>active</em> volume's drives, so drives on other volumes will never
	 * have the file.
	 * </p>
	 * <p>
	 * Each drive is attempted independently: a genuine I/O error on one drive is
	 * logged but must not abort the loop, otherwise the journal file would remain
	 * on all subsequent drives and be replayed again on the next restart.
	 * </p>
	 */
	public void removeJournal(String id) {
		Check.requireNonNullArgument(id, "id is null");
		getLockService().getJournalLock().writeLock().lock();
		try {
			for (Drive drive : getDriver().getDrivesEnabled()) {
				try {
					drive.removeJournal(id);
				} catch (Exception e) {
					// Log and continue: a failure on one drive must not prevent deletion
					// from all remaining drives (including the volume that actually holds
					// the journal file). The caller (OdilonJournalService) already treats
					// a partial-delete gracefully.
					logger.error("removeJournal: failed on drive -> " + drive.getName() + " | id=" + id + " | " + e.getMessage(), SharedConstant.NOT_THROWN);
				}
			}
		} finally {
			getLockService().getJournalLock().writeLock().unlock();
		}
	}

	public List<VirtualFileSystemOperation> getJournalPending() {

		List<VirtualFileSystemOperation> list = new ArrayList<VirtualFileSystemOperation>();

		getLockService().getJournalLock().writeLock().lock();
		try {
			for (Drive drive : getDriver().getDrivesEnabled()) {
				File dir = new File(drive.getJournalDirPath());
				if (!dir.exists()) {
					// In multi-volume Erasure Coding, saveJournal writes only to the active
					// volume's
					// drives, so earlier volumes' journal directories may legitimately be empty;
					// after a filesystem issue any drive's directory may be temporarily gone.
					// `return list` here would silently drop every entry from all subsequent
					// drives — including the volume that actually holds the journal file.
					// Skip this drive and keep scanning the rest.
					logger.debug("getJournalPending: journal dir does not exist on drive -> " + drive.getName() + " | " + dir.getAbsolutePath() + " — skipping");
					continue;
				}
				if (!dir.isDirectory()) {
					logger.debug("getJournalPending: journal dir path is not a directory on drive -> " + drive.getName() + " | " + dir.getAbsolutePath() + " — skipping");
					continue;
				}
				File[] files = dir.listFiles();
				// listFiles() returns null on I/O error or if the directory disappears between
				// the isDirectory() check above and this call. A null here would throw NPE
				// and abort recovery for every subsequent drive / volume — guard against it.
				if (files == null) {
					logger.debug("getJournalPending: listFiles() returned null for journal dir -> " + dir.getAbsolutePath() + " (I/O error or directory disappeared)");
					continue;
				}
				for (File file : files) {
					if (!file.isDirectory()) {
						Path pa = Paths.get(file.getAbsolutePath());
						try {
							String str = Files.readString(pa);
							OdilonVirtualFileSystemOperation op = getObjectMapper().readValue(str, OdilonVirtualFileSystemOperation.class);
							if (op != null) {
								op.setJournalService(getJournalService());
								if (!list.contains(op)) {
									list.add(op);
									logger.debug("added to rollback -> " + op.toString());
								}
							}

						} catch (IOException e) {
							try {
								Files.delete(file.toPath());
							} catch (IOException e1) {
								logger.error(e, SharedConstant.NOT_THROWN);
							}
						}
					}
				}
			}
			std_logger.info("Total operations that will rollback -> " + String.valueOf(list.size()));
			return list;

		} finally {
			getLockService().getJournalLock().writeLock().unlock();
		}
	}

	@Override
	protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
		return getRAIDSixDriver().getObjectMetadataReadDrive(bucket, objectName);
	}

	private ECDriver getRAIDSixDriver() {
		return this.driver;
	}

}
