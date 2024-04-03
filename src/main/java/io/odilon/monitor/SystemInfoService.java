/*
 * Odilon Object Storage
 * (C) Novamens 
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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;


import io.odilon.OdilonVersion;
import io.odilon.log.Logger;
import io.odilon.model.BaseService;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SystemInfo;
import io.odilon.service.ServerSettings;
import io.odilon.service.SystemService;
import io.odilon.vfs.model.Drive;
import io.odilon.vfs.model.VirtualFileSystemService;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class SystemInfoService extends BaseService implements SystemService {
		
	static private Logger logger = Logger.getLogger(SystemInfoService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");
				
	private final OffsetDateTime started = OffsetDateTime.now(); 
	
	private Integer availableProcessors;
	private Long maxMemory;
	private Long totalMemory;
	private String osName;
	private String osVersion;
	private String userName;
	private String userProfile;
	private String javaVersion;
	private String appVersion;
	private String serverHost;
	
	private String serverMode;
	private String serverDataStorageMode;
	
	private String isEncryptEnabled;
	private String isEncryptionInitialized;
	
	private String isStandby;
	private String standbyUrl;
	private String standbyPort;
	
	private String isVaultEnabled;
	private String vaultUrl;
	
	private String isVersionControl;
	
	
	@JsonIgnore
	@Autowired
	private ServerSettings serverSettings;
	
	@JsonIgnore
	private VirtualFileSystemService virtualFileSystemService;
	
	public SystemInfoService(ServerSettings serverSettings, VirtualFileSystemService virtualFileSystemService) {
		this.serverSettings=serverSettings;
		this.virtualFileSystemService=virtualFileSystemService;
	}
	
	/**
	 * 
	 */
	public SystemInfo getSystemInfo() {
		
		SystemInfo info = new SystemInfo();

		info.javaHome=System.getProperty("java.home");
		info.javaVendor=System.getProperty("java.vendor");
		info.userHome=System.getProperty("user.home");
		info.osArch=System.getProperty("os.arch");
		
		info.availableProcessors = availableProcessors;
		info.maxMemory = maxMemory;
		info.totalMemory=totalMemory;
		info.osName = osName;
		info.osVersion = osVersion;
		info.started=started;

		info.userName=userName;
		info.userProfile=userProfile;
		info.userDir=System.getProperty("user.dir");
		info.userProfile=System.getProperty("user.profile");
		
		
		info.javaVersion = javaVersion;
		info.appVersion = appVersion;
		info.serverHost = serverHost;
		info.javaHome = System.getProperty("java.home");
		
		info.serverMode = serverMode;
		
		info.serverDataStorageMode=serverDataStorageMode;
		
		info.isVaultEnabled = isVaultEnabled;
		info.vaultUrl=vaultUrl;
		
		info.isVersionControl = isVersionControl;
		
		info.isEncryptEnabled = isEncryptEnabled;
		
		info.isEncryptionInitialized = isEncryptionInitialized;
		
		info.isStandby = isStandby;
		info.standbyUrl = standbyUrl;
		info.standbyPort = standbyPort;
		
		info.freeMemory=Runtime.getRuntime().freeMemory();
		info.redundancyLevel=serverSettings.getRedundancyLevel();
		
		if (serverSettings.getRedundancyLevel()==RedundancyLevel.RAID_6) {
			info.redundancyLevelDetail = "[data:" + String.valueOf(serverSettings.getRAID6DataDrives()) + 
										 ", parity:" + String.valueOf(serverSettings.getRAID6ParityDrives())+"] "; 
		}
		
		OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
		
		if (os.getSystemLoadAverage()>0)
			info.cpuLoadAverage=Double.valueOf(os.getSystemLoadAverage());
		
		List<Long> available = new ArrayList<Long>();
		Long total;
		
		if(serverSettings.getRedundancyLevel()==RedundancyLevel.RAID_0) {
			/** 
			 * for RAID 0 the total storage is the smallest disk  by the number of disks 
			 * */
			this.virtualFileSystemService.getMapDrivesEnabled().values().forEach(item -> available.add(Long.valueOf(item.getAvailableSpace())));
			Long min = available.stream().map(d -> d).reduce((available.size()>0? available.get(0):0), (a, b) -> (a<b?a:b));
			total =  min * available.size();
			info.availableDisk = total;
		}
		else if(serverSettings.getRedundancyLevel()==RedundancyLevel.RAID_1) {
			/** 
			 * for RAID 1 the total storage is the smallest disk 
			 * */
			this.virtualFileSystemService.getMapDrivesEnabled().values().forEach(item -> available.add(Long.valueOf(item.getAvailableSpace())));
			Long min = available.stream().map(d -> d).reduce((available.size()>0? available.get(0):0), (a, b) -> (a<b?a:b));
			info.availableDisk = min;
		}
		else if(serverSettings.getRedundancyLevel()==RedundancyLevel.RAID_6) {
			/** 
			 * RAID 6 
			 * TBA
			 * 
			 * */
			total = Long.valueOf(0); //Long.valueOf(available.stream().map(d -> d).reduce(Long.valueOf(0), (a, b) -> (a<b?a:b)) * available.size() * 2/3 );
			info.availableDisk = total;
		}
		
		Map<String, Long> totalStorage = new HashMap<String, Long>();
		Map<String, Long> availableStorage = new HashMap<String, Long>();
		
		for (Path root : FileSystems.getDefault().getRootDirectories()) {
		    try {
		    	FileStore store = Files.getFileStore(root);
		    	totalStorage.put(root.toFile().getAbsolutePath(), Long.valueOf(store.getTotalSpace()));
		    	availableStorage.put(root.toFile().getAbsolutePath(), Long.valueOf(store.getUsableSpace()));
		    } catch (IOException e) {
		    	logger.warn(e, ServerConstant.NOT_THROWN);
		    }
		}
		
		info.totalStorage = totalStorage; 
		info.serverStorage =availableStorage;
		
		/** -----------------------
		info.odilonServerId;
		-------------------------**/
							
		Map<String, Long> driveTotalStorage = new HashMap<String, Long>();
		Map<String, Long> driveAvailableStorage = new HashMap<String, Long>();
	
		for (Drive drive: this.virtualFileSystemService.getMapDrivesEnabled().values() ) {
		    try {
		    	Path path = (new File(drive.getRootDirPath())).toPath();
		    	FileStore store = Files.getFileStore(path);
		    	driveTotalStorage.put(path.toFile().getName(), Long.valueOf(store.getTotalSpace()));
		    	driveAvailableStorage.put(path.toFile().getName(), Long.valueOf(store.getUsableSpace()));
		    } catch (IOException e) {
		    	logger.warn(e, ServerConstant.NOT_THROWN);
		    }
		}
		return info;
	}

	
	/**
	 * 
	 */
	@PostConstruct
	private void onInitialize() {
	
		synchronized (this) {
			
			setStatus(ServiceStatus.STARTING);

			try {
				this.availableProcessors = Integer.valueOf(Runtime.getRuntime().availableProcessors());
				this.maxMemory=Long.valueOf(Runtime.getRuntime().maxMemory());
				this.totalMemory=Long.valueOf(Runtime.getRuntime().totalMemory());
				this.osName=System.getProperty("os.name");
				this.osVersion=System.getProperty("os.version");
				
				this.userName=System.getenv().get("USERNAME");
				this.userProfile=System.getenv().get("USERPROFILE");
				
				this.javaVersion=System.getProperty("java.specification.version");
				this.appVersion=OdilonVersion.VERSION;
				this.serverHost=getServerHost();
				
				this.serverMode=serverSettings.getServerMode();
				
				this.serverDataStorageMode=serverSettings.getDataStorage().getName();
	
				this.isVersionControl = serverSettings.isVersionControl() ? "true" : "false";
				this.isEncryptEnabled = serverSettings.isEncryptionEnabled() ? "true" : "false";
				
				this.isVaultEnabled=serverSettings.isVaultEnabled()?"true":"false";
				if (serverSettings.isVaultEnabled()) {
					this.vaultUrl=serverSettings.getVaultUrl().isPresent()?serverSettings.getVaultUrl().get():"";
				}
				
				this.isStandby=serverSettings.isStandByEnabled()?"true":"false";
				
				if (serverSettings.isStandByEnabled()) {
					this.standbyUrl=serverSettings.getStandbyUrl();
					this.standbyPort= String.valueOf(serverSettings.getStandbyPort());
				}
				
				if (this.virtualFileSystemService.getOdilonServerInfo()!=null)
					this.isEncryptionInitialized = this.virtualFileSystemService.getOdilonServerInfo().isEncryptionIntialized()?"true":"false";
				
				
			} catch (Exception e) {
				logger.error(e);
			}
			startuplogger.debug("Started -> " + SystemInfoService.class.getSimpleName());
			setStatus(ServiceStatus.RUNNING);
		}
	}
	
	/**
	 * @return
	 */
	@SuppressWarnings("unused")
	private String getServerHost() {
		
		StringBuilder output = new StringBuilder();
		
		try {
			
			ProcessBuilder processBuilder = new ProcessBuilder();

			if (isLinux())
				processBuilder.command("bash", "-c", "hostname");
			else
				processBuilder.command("cmd.exe", "/c", "hostname");
			
			Process process = processBuilder.start();
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

			String line;
			
			while ((line = reader.readLine()) != null) {
				output.append(line);
			}
			int exitVal = process.waitFor();

		} catch (Exception e) {
			return e.getClass().getName();
		}
		return output.toString();
	}
	
	private static boolean isLinux() {
		if  (System.getenv("OS")!=null && System.getenv("OS").toLowerCase().contains("windows")) 
			return false;
		return true;
	}
}






