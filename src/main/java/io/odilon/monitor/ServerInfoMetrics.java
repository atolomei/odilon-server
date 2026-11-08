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

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.odilon.OdilonVersion;
import io.odilon.log.Logger;
import io.odilon.model.RedundancyLevel;
import io.odilon.service.ServerSettings;

/**
 * <p>
 * Publishes the (static) server configuration for Grafana as an "info"
 * metric: a gauge that is always 1 and whose labels carry the values.
 * </p>
 * 
 * <pre>
 * odilon_server_info{version="3.1", redundancy="RAID 0", server_mode="master", ...} 1
 * </pre>
 * <p>
 * Only an explicit whitelist of non-sensitive settings is published. Never add
 * access/secret keys, encryption keys or Vault credentials here.
 * </p>
 * <p>
 * The numeric settings that are useful to graph or alert on are also published
 * as plain gauges. The start time and uptime are already provided by Spring
 * Boot ({@code process_start_time_seconds}, {@code process_uptime_seconds}).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
public class ServerInfoMetrics {

	static private Logger logger = Logger.getLogger(ServerInfoMetrics.class.getName());

	private final MeterRegistry registry;
	private final ServerSettings settings;

	public ServerInfoMetrics(MeterRegistry registry, ServerSettings settings) {
		this.registry = registry;
		this.settings = settings;
	}

	/** after startup, so every setting has been loaded and derived */
	@EventListener(ApplicationReadyEvent.class)
	public void register() {
		try {

			Gauge.builder("odilon.server.info", this, x -> 1.0).description("Odilon server configuration (value is always 1; see labels)").tags(infoTags()).register(registry);


			Gauge.builder("odilon.server.base.info", this, x -> 1.0).description("Odilon server hardware and OS configuration (value is always 1; see labels)").tags(baseTags()).register(registry);

			
			Gauge.builder("odilon.traffic.tokens", settings, s -> s.getMaxTrafficTokens()).description("Max concurrent API requests (traffic.tokens)").register(registry);

			Gauge.builder("odilon.encryption.record.size.bytes", settings, s -> s.getEncryptionRecordSize()).baseUnit("bytes").description("Encryption record size").register(registry);

		} catch (Exception e) {
			/** metrics must never prevent the server from starting */
			logger.error(e);
		}
	}

	private Tags infoTags() {

		String redundancy;
		if (settings.getRedundancyLevel() == RedundancyLevel.ERASURE_CODING)
			redundancy = "Erasure coding [" + settings.getECDataDrives() + "," + settings.getECParityDrives() + "]";
		else
			redundancy = settings.getRedundancyLevel() != null ? settings.getRedundancyLevel().getName() : "-";

		List<String> dirs = settings.getRootDirs();
		StringBuilder rootDirs = new StringBuilder();
		if (dirs != null) {
			int n = 0;
			for (String d : dirs) {
				if (n > 0)
					rootDirs.append(", ");
				rootDirs.append(n++).append(": ").append(d);
			}
		}

		int recordSize = settings.getEncryptionRecordSize() / 1000;
		
	 
		
		
		return Tags.of(
				"version", nz(OdilonVersion.VERSION),
				"redundancy", redundancy,
				"server_mode", nz(settings.getServerMode()),
				"version_control", settings.getVersionControl() != null ? settings.getVersionControl().getName() : "-",
				"data_immutability", settings.getDataStorage() != null ? settings.getDataStorage().getName() : "-",
				"master_standby", settings.isStandByEnabled() ? "enabled" : "disabled",
				"https", String.valueOf(settings.isHTTPS()),
				"port", String.valueOf(settings.getPort()),
				"encryption_enabled", String.valueOf(settings.isEncryptionEnabled()),
				"encrypt_metadata", String.valueOf(settings.isEncryptMetadata()),
				"encryption_record_size", String.valueOf(recordSize) + " KB",
				"vault_enabled", String.valueOf(settings.isVaultEnabled()),
				"root_dirs", rootDirs.toString(),
				"traffic_tokens", String.valueOf(settings.getMaxTrafficTokens())
				
				
				);
	}
	
	private Tags baseTags() {

		
		Runtime rt = Runtime.getRuntime();
		
		RuntimeMXBean rmx = ManagementFactory.getRuntimeMXBean();
		
		
		return Tags.of(

				"available_processors", rt.availableProcessors() + " cores",
				"free_memory",   gb(rt.freeMemory()),
				"maximun_memory",   gb(rt.maxMemory()),
				"total_memory",   gb(rt.totalMemory()),
				"jvm_spec", nz(System.getProperty("java.specification.version")),
				"os", nz(System.getProperty("os.name")),
				"os_version", nz(System.getProperty("os.version")),
				"os_arch", nz(System.getProperty("os.arch")),
				"profile", nz(System.getProperty("spring.profiles.active")),
				"server",  nz(System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : System.getenv("COMPUTERNAME")),
				"user.country", nz(System.getProperty("user.country")),
				"user.dir",  nz(System.getProperty("user.dir")),
				"user.home",  nz(System.getProperty("user.home")),
				"user.language", nz(System.getProperty("user.language")),
				
				"jvm_runtime",  nz(System.getProperty("java.vendor")) + " " + nz(rmx.getVmName()) + " " + nz(System.getProperty("java.version")) + " " + nz(rmx.getVmVersion()),
				"jvm_args", String.join(" ", rmx.getInputArguments()),
				"started",  DateTimeFormatter.RFC_1123_DATE_TIME.format(settings.getSystemStartTime())
		
				
				);
	}
	

	private String nz(String s) {
		return s != null ? s : "-";
	}
	
 	private static String gb(long bytes) {
		return String.format("%.4f GB", bytes / GB);
	}
	private static final double GB = 1024.0 * 1024.0 * 1024.0;
	
	
	
}
