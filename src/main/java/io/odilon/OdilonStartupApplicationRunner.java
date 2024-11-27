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
package io.odilon;

import java.io.File;
import java.time.OffsetDateTime;
import java.util.Locale;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.replication.ReplicationService;
import io.odilon.scheduler.CronJobDataIntegrityCheckRequest;
import io.odilon.scheduler.CronJobWorkDirCleanUpRequest;
import io.odilon.scheduler.PingCronJobRequest;
import io.odilon.scheduler.SchedulerService;
import io.odilon.security.VaultService;
import io.odilon.service.ServerSettings;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>This class is executed after {@link OdilonApplication} startup but <b>before</b> 
 * it starts accepting traffic.</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
public class OdilonStartupApplicationRunner implements ApplicationRunner {
	
	static private Logger logger = Logger.getLogger(OdilonApplication.class.getName());
	static private Logger startupLogger = Logger.getLogger("StartupLogger");

	@JsonIgnore
	private final ApplicationContext appContext;

	@JsonIgnore
	@Autowired
	private SchedulerService schedulerService;

	
	/**
	 * 
	 * @param appContext 
	 * @param schedulerService  
	 */
	public OdilonStartupApplicationRunner(ApplicationContext appContext, SchedulerService schedulerService) {
		this.appContext = appContext;
		this.schedulerService=schedulerService;
	}
	
	
	@Override
	public void run(ApplicationArguments args) throws Exception {

		if (startupLogger.isDebugEnabled()) {
			startupLogger.debug("Command line args:");
			args.getNonOptionArgs().forEach( item -> startupLogger.debug(item));
			startupLogger.debug(ServerConstant.SEPARATOR);
		}

		Locale.setDefault(Locale.ENGLISH);
		
		initCronJobs();
		
		boolean iGeneral = initGeneral();
		if(iGeneral)
			startupLogger.info(ServerConstant.SEPARATOR);
		
		boolean iVault = initVault();
		if(iVault)
			startupLogger.info(ServerConstant.SEPARATOR);
		
		boolean iKeys = initKeys();
		if(iKeys)
			startupLogger.info(ServerConstant.SEPARATOR);
		
		boolean iStandby = initStandby();
		if(iStandby)
			startupLogger.info(ServerConstant.SEPARATOR);
		
		startupLogger.info	("Startup at -> " + OffsetDateTime.now().toString());
	}
	
	public SchedulerService getSchedulerService() {
		return schedulerService;
	}

	public void setSchedulerService(SchedulerService schedulerService) {
		this.schedulerService = schedulerService;
	}

	public ApplicationContext getAppContext() {
		return appContext;
	}
	
	/**
	 * 
	 */
	private void initCronJobs() {
										
		ServerSettings settingsService = getAppContext().getBean(ServerSettings.class);
		
		/** Integrity Checks **/
		if (settingsService.isIntegrityCheck()) {
			CronJobDataIntegrityCheckRequest checker = appContext.getBean(CronJobDataIntegrityCheckRequest.class, settingsService.getIntegrityCheckCronExpression());
			getSchedulerService().enqueue(checker);
			startupLogger.debug("Integrity Check -> " 		+
								"CronExpression: " 			+ settingsService.getIntegrityCheckCronExpression() + " | " +
								"Checking interval (days) " +  String.valueOf(settingsService.getIntegrityCheckDays()) + " | " +
								"Threads " + String.valueOf(settingsService.getIntegrityCheckThreads()));
		}
		else {
			startupLogger.debug("Integrity Check -> disabled");	
		}

		/** Clean up work dirs 
		 *  Default -> Once per hour
		 * **/
		String cronJobWorkDirCleanUp = settingsService.getCronJobWorkDirCleanUp();
		CronJobWorkDirCleanUpRequest cleanUpDirs = getAppContext().getBean(CronJobWorkDirCleanUpRequest.class, cronJobWorkDirCleanUp);
		getSchedulerService().enqueue(cleanUpDirs);

		/** Ping **/
		if (settingsService.isPingEnabled()) {
			String cronJobPing = settingsService.getCronJobcronJobPing();
			PingCronJobRequest ping = getAppContext().getBean(PingCronJobRequest.class, cronJobPing);
			getSchedulerService().enqueue(ping);
			startupLogger.debug("Ping -> " + "CronExpression: " + cronJobPing);
		}
	}
	
	
		
	/**
	 *
	 * 
	 */
	private boolean initGeneral() {
		ServerSettings settingsService = getAppContext().getBean(ServerSettings.class);
		
		startupLogger.info("Https -> " + (settingsService.isHTTPS() ? "true" : "false"));
		startupLogger.info("Port-> " + String.valueOf(settingsService.getPort()));
		
		OdilonServerInfo info = getAppContext().getBean(VirtualFileSystemService.class).getOdilonServerInfo();
		
		startupLogger.info("Encryption service initialized -> " + ( ((info!=null) && info.isEncryptionIntialized()) ? "true" :"false" ));
		startupLogger.info("Encryption enabled -> " + String.valueOf(settingsService.isEncryptionEnabled()));
		startupLogger.info("Version Control -> " + String.valueOf(settingsService.isVersionControl()));
		startupLogger.info("Data Storage mode -> " + settingsService.getDataStorage().getName());
		
		if (settingsService.getRedundancyLevel()==RedundancyLevel.RAID_6) {
			startupLogger.info("Data Storage redundancy level -> " + settingsService.getRedundancyLevel().getName()+
					" [data:"+ String.valueOf(settingsService.getRAID6DataDrives())+
					", parity:" + String.valueOf(settingsService.getRAID6ParityDrives())+"]");
		}
		else
			startupLogger.info("Data Storage redundancy level -> " + settingsService.getRedundancyLevel().getName());
		getAppContext().getBean(VirtualFileSystemService.class).getMapDrivesEnabled().forEach((k,v) -> startupLogger.info("Drive: " + k +" | rootDir: " + v.getRootDirPath()));
		return true;
	}
	
	/**
	 * 
	 */
	private boolean initVault() {
		
		ServerSettings settingsService = getAppContext().getBean(ServerSettings.class);
		boolean isOk = true;
		
		startupLogger.info("Vault enabled -> " + String.valueOf(settingsService.isVaultEnabled()));
		startupLogger.info("Vault use for new files -> " + String.valueOf(settingsService.isUseVaultNewFiles()));
		
		if (settingsService.isVaultEnabled()) {
			if (settingsService.getVaultUrl().isPresent()) {
				String ping = getAppContext().getBean(VaultService.class).ping();
				if (ping==null || !ping.equals("ok")) {
					startupLogger.error();
					startupLogger.error(ping);
					startupLogger.error();
					isOk = false;
				}
				else {
					startupLogger.info("Vault connection -> " + settingsService.getVaultUrl().get());
					startupLogger.info("Vault connection status -> " + ping);
					isOk = true;
				}
			}
			else {
				startupLogger.error("Vault is enabled but vault.url is null" );
				isOk = false;
			}
				if (!isOk) {
						startupLogger.error("The system can not run without a Vault operational");
						startupLogger.error("Check variable 'vault.url' and 'vault' in -> ." + File.separator+ "config" + File.separator + "odilon.properties" );
						startupLogger.error("Current value for vault.enabled = " + settingsService.isVaultEnabled());
						startupLogger.error("Current value for vault.newfiles = " + settingsService.isUseVaultNewFiles());
						startupLogger.error("Current value for vault.url -> " + settingsService.getVaultUrl().get());
						startupLogger.error("You must set vault.enabled=false if there is no Vault available" );
						startupLogger.error("Vault related variables (with sample values): ");
						startupLogger.error("# no Vault at all, reagrdless of the other variables" );
						startupLogger.error("vault.enabled=false" );
						
						startupLogger.error("# What to do with new files, whether to use Vault or not" );
						startupLogger.error("vault.newfiles=false" );
						startupLogger.error("vault.url=http://127.0.0.1:8220" );
						startupLogger.error("vault.roleId=01aa9d05-1a0a-1392-5a90-784819064f05" );
						startupLogger.error("vault.secretId=xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" );
						startupLogger.error("vault.keyId=kbee-key" );
						
						startupLogger.error("Exiting");
						startupLogger.error(ServerConstant.SEPARATOR);
						try {
							Thread.sleep(5000);
						} catch (InterruptedException e) {
						}
						((ConfigurableApplicationContext) getAppContext().getBean(VirtualFileSystemService.class).getApplicationContext()).close();
						System.exit(1);
						
				}
				return true;
		}
		return true;
	}
	
	/**
	 * 
	 * 
	 */
	private boolean initKeys() {
		ServerSettings settingsService = getAppContext().getBean(ServerSettings.class);
		if (settingsService.getAccessKey().equals("odilon") && settingsService.getSecretKey().equals("odilon")) {
			startupLogger.info("Odilon is running with default vaules for AccessKey and SecretKey (ie. odilon/odilon)");
			startupLogger.info("It is recommended to change their values in file -> ."+File.separator + "config"+ File.separator +"odilon.properties");
			return true;
		}	
		return false;
	}
	

	/**
	 * 
	 * 
	 */
	private boolean initStandby() {
		ServerSettings settingsService = getAppContext().getBean(ServerSettings.class);
		if (settingsService.getServerMode().equals(ServerConstant.STANDBY_MODE)) {
			startupLogger.info("Server is running in mode -> " + settingsService.getServerMode());
			startupLogger.info(ServerConstant.SEPARATOR);
		}
		else {
			ReplicationService replicationService = getAppContext().getBean(ReplicationService.class);
			if (settingsService.isStandByEnabled()) {
				startupLogger.info("Standby Server -> enabled");
				
				startupLogger.info("Standby connection -> " + replicationService.getStandByConnection());
				String ping = replicationService.pingStandBy();
				if (ping.equals("ok")) { 
					startupLogger.info("Standby connection status -> " + ping);
					
					if (settingsService.isVersionControl() && (!replicationService.isVersionControl())) {
						
						startupLogger.error("Server has Version Control enabled but Standby replica does not. You must either:");
						startupLogger.error("- Disable Version Control in Master Server");
						startupLogger.error("- Enable Version Control in Standby Server");
						startupLogger.error("- Disable Standby replication");
						
						startupLogger.error("The server can not continue.");
						
						((ConfigurableApplicationContext) getAppContext().getBean(VirtualFileSystemService.class).getApplicationContext()).close();
						System.exit(1);
					}
				}
				else {
					startupLogger.error("Standby connection  error -> " + ping);
					startupLogger.error("The server is set up to use a standby connection that is not available");
					startupLogger.error("You must check the connection or disable standby replica in file -> ."+File.separator + "config"+ File.separator +"odilon.properties");
					startupLogger.error("Current value for standby.enabled -> " + settingsService.isStandByEnabled());	
					startupLogger.error("Exiting");
					startupLogger.error(ServerConstant.SEPARATOR);
					try {
						Thread.sleep(2500);
					} catch (InterruptedException e) {
					}
					((ConfigurableApplicationContext) getAppContext().getBean(VirtualFileSystemService.class).getApplicationContext()).close();
					System.exit(1);
				}
				try {
					replicationService.checkStructure();
				} catch (Exception e) {
					logger.error(e.getClass().getName() + " | " + e.getMessage());
					
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e1) {
					
					}
					
					logger.error("You will have to check the standby server to enable replication");
					logger.error("Meanwhile we recommend to startup the server without it "
							   + " in ./config/odilon.properties -> standby.enabled=false ");
					System.exit(1);
				}
				
			}
		}
		return false;
	}
}



