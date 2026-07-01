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
package io.odilon.virtualFileSystem.raid0;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.BaseRAIDHandler;
import io.odilon.virtualFileSystem.OdilonVirtualFileSystemOperation;
import io.odilon.virtualFileSystem.RAIDHandler;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.IODriver;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;

public class RAIDZeroJournalHandler extends BaseRAIDHandler implements RAIDHandler {

	@JsonIgnore
	private final RAIDZeroDriver driver;

	static private Logger logger = Logger.getLogger(RAIDZeroDriver.class.getName());
	static private Logger std_logger = Logger.getLogger("StartupLogger");

	public RAIDZeroJournalHandler(RAIDZeroDriver driver) {
		this.driver = driver;
	}

	@Override
	public IODriver getDriver() {
		return driver;
	}

	@Override
	protected Drive getObjectMetadataReadDrive(ServerBucket bucket, String objectName) {
		return this.driver.getReadDrive(bucket, objectName);
	}

	public void saveJournal(VirtualFileSystemOperation operation) {
		getDriver().getDrivesEnabled().get(0).saveJournal(operation);

	}

	public void removeJournal(String id) {
		getDriver().getDrivesEnabled().get(0).removeJournal(id);
	}

	/**
	 * JournalService journalService =
	 * getVirtualFileSystemService().getApplicationContext().getBean(JournalService.class)
	 * 
	 * @return
	 */
	public List<VirtualFileSystemOperation> getJournalPending() {

		List<VirtualFileSystemOperation> list = new ArrayList<VirtualFileSystemOperation>();
		Drive drive = getDriver().getDrivesEnabled().get(0);

		File dir = new File(drive.getJournalDirPath());

		if (!dir.exists())
			return list;

		if (!dir.isDirectory())
			return list;

		File[] files = dir.listFiles();

		if (files.length == 0)
			return list;

		for (File file : files) {

			if (!file.isDirectory()) {
				Path pa = Paths.get(file.getAbsolutePath());
				try {
					String str = Files.readString(pa);
					OdilonVirtualFileSystemOperation operation = getObjectMapper().readValue(str, OdilonVirtualFileSystemOperation.class);
					operation.setJournalService(getJournalService());
					list.add(operation);
				} catch (IOException e) {
					logger.debug(e, getDriver().fileInfo(file));
					try {
						Files.delete(file.toPath());
					} catch (IOException e1) {
						logger.error(e, SharedConstant.NOT_THROWN);
					}
				}
			}
		}
		std_logger.info("Rollback -> " + String.valueOf(list.size()) + " transactions");
		return list;
	}

}
