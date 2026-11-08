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
package io.odilon.monitor;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.odilon.log.Logger;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Publishes the disk space of every Odilon drive as Micrometer gauges
 * (Prometheus / Grafana):
 * </p>
 * <ul>
 * <li>{@code odilon_drive_total_bytes{drive,path}}</li>
 * <li>{@code odilon_drive_available_bytes{drive,path}}</li>
 * </ul>
 * <p>
 * Values are those of the file system that holds each drive (not the bytes
 * used by Odilon objects only), read on every scrape. Used space =
 * {@code total - available}.
 * </p>
 * <p>
 * Registered once the application is ready, because the drives are loaded by
 * the {@link VirtualFileSystemService} after the monitoring beans are created.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
public class DriveMetrics {

	static private Logger logger = Logger.getLogger(DriveMetrics.class.getName());

	private final MeterRegistry registry;
	private final VirtualFileSystemService vfs;

	public DriveMetrics(MeterRegistry registry, @Lazy VirtualFileSystemService vfs) {
		this.registry = registry;
		this.vfs = vfs;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void registerDrives() {
		try {
			for (Drive drive : vfs.getMapDrivesAll().values())
				register(drive);
		} catch (Exception e) {
			/** metrics must never prevent the server from starting */
			logger.error(e);
		}
	}

	/**
	 * Idempotent: Micrometer returns the existing gauge if the same name and tags
	 * are registered again. The gauge keeps a weak reference to the Drive, which is
	 * held by the VirtualFileSystemService.
	 */
	private void register(Drive drive) {

		String name = drive.getName();
		String path = drive.getRootDirPath();

		Gauge.builder("odilon.drive.total.bytes", drive, Drive::getTotalSpace)
				.tag("drive", name).tag("path", path).baseUnit("bytes")
				.description("Total size of the file system that holds the drive")
				.register(registry);

		Gauge.builder("odilon.drive.available.bytes", drive, Drive::getAvailableSpace)
				.tag("drive", name).tag("path", path).baseUnit("bytes")
				.description("Available (usable) space of the file system that holds the drive")
				.register(registry);
	}
}
