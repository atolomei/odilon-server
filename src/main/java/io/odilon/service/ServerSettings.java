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
package io.odilon.service;

import java.io.File;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import io.odilon.OdilonVersion;
import io.odilon.encryption.EncryptionService;
import io.odilon.model.RAID6Config;
import io.odilon.log.Logger;
import io.odilon.model.JSONObject;
import io.odilon.model.DataStorage;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.VersionControl;
import io.odilon.service.util.ByteToString;
import io.odilon.util.RandomIDGenerator;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Server configuration defined in file {@code odilon.properties}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */
@Configuration
@PropertySource("classpath:odilon.properties")
public class ServerSettings implements JSONObject {

	static private Logger logger = Logger.getLogger(ServerSettings.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	static private final RandomIDGenerator idGenerator = new RandomIDGenerator();

	private static final OffsetDateTime systemStarted = OffsetDateTime.now();

	protected String version = "";

	private String encryptionKey;
	private String encryptionIV;

	// PING ------------------------

	@Value("${ping.enabled:true}")
	protected boolean pingEnabled;

	@Value("${ping.email.enabled:false}")
	protected boolean pingEmailEnabled;

	public boolean isPingEmailEnabled() {
		return this.pingEmailEnabled;
	}

	@Value("${ping.email.address:null}")
	protected String pingEmailAddress;

	public String isPingEmailAddress() {
		return this.pingEmailAddress;
	}

	@Value("${ping.cronJobcronJobPing:45 * * * * *}")
	protected String cronJobcronJobPing;

	public String getCronJobcronJobPing() {
		return cronJobcronJobPing;
	}

	private RedundancyLevel redundancyLevel;

	// SERVER ------------------------

	/* default -> odilon */
	@Value("${accessKey:odilon}")
	protected String accessKey;

	/* default -> odilon */
	@Value("${secretKey:odilon}")
	protected String secretKey;

	/* default port -> 9234 */
	@Value("${server.port:9234}")
	protected int port;

	@Value("${timezone:null}")
	protected String timeZone;

	@Value("${server.mode:master}") /** server.mode = master | standby */
	protected String serverMode;

	
	/** Version Control. by default disabled **/
	@Deprecated
	@Value("${server.versionControl:null}")
	protected String serverVersionControlStr;
	// protected boolean versionControl;

	
	/** Version Control. by default disabled  **/
	@Value("${version.control:null}")
	protected String versionControlStr;
	// protected boolean versionControl;

	protected VersionControl versionControlModel;

	
	@Value("${recovery:false}")
	protected boolean recovery = false;

	@Value("${server.ssl.enabled:false}")
	protected String ishttps;

	@Value("${server.objectstream.cache.secs:3600}")
	protected int serverObjectstreamCacheSecs;

	// ENCRYPTION -----------------------------------------------
	//
	// by default encryption is not enabled
	//
	@Value("${encryption.enabled:false}")
	protected boolean isEncrypt;

	/** When true, ObjectMetadata JSON files are encrypted on disk. by default this field mirrors data encryption setting,
	 * if encryption.enabled=true and encrypt.metadata will be true by default
	 * if encryption.enabled=false, then metadata encryption is disabled regardless of the value of encrypt.metadata 
	 * */
	
   /*  Requires encryption.enabled=true to have any effect. */
	@Value("${encrypt.metadata:null}")
	protected String encryptMetadataStr;

	protected boolean encryptMetadata;

	@Value("${encryption.key:#{null}}")
	protected String encryptionKeyIV;

	@Value("${encryption.masterkey:#{null}}")
	protected String masterkey_lowercase;

	@Value("${encryption.masterKey:#{null}}")
	protected String masterKey;

	@Value("${encryption.keyAlgorithm:AES}")
	protected String keyAlgorithm;

	@Value("${presignedSalt:pxUrktu3oMo$dfgh9rl!}")
	protected String presignedSalt;

	// DATA STORAGE -------------------------------------------
	//
	//
	@Value("${redundancyLevel:null}")
	protected String redundancyLevelStr;

	/**
	 * Legacy flat drive list.  Used by RAID 0, RAID 1 and as a backward-compatible
	 * fallback for RAID 6.  For new RAID 6 deployments use
	 * {@code dataStorage.volume.N=} instead.
	 * Default is empty so the property is optional when the per-volume syntax is used.
	 */
	@Value("#{'${dataStorage:}'.split(',')}")
	private List<String> rootDirs;

	/**
	 * Per-volume ordered drive lists, populated from
	 * {@code dataStorage.volume.0=...}, {@code dataStorage.volume.1=...}, etc.
	 * Empty for RAID 0 / RAID 1 and for legacy RAID 6 deployments.
	 */
	private List<List<String>> volumeRootDirs = new ArrayList<>();

	/**
	 * Data shards per volume — derived from {@link RAID6Config}, never user-configured.
	 */
	protected int raid6DataDrives = -1;

	/**
	 * Parity shards per volume — derived from {@link RAID6Config}, never user-configured.
	 */
	protected int raid6ParityDrives = -1;

	/**
	 * Number of volumes — derived from the number of {@code dataStorage.volume.*}
	 * keys (new style) or from the legacy {@code raid6.volumes} property.
	 */
	protected int raid6Volumes = 1;

	@Autowired
	private Environment environment;

	@Value("${dataStorageMode:rw}")
	protected String dataStorageMode;
	/** readwrite, readonly, WORM */

	/**
	 * <p>
	 * The 0-based id of the RAID 6 volume that receives new-object writes.
	 * </p>
	 * <p>
	 * Controlled by the {@code volume.active} property in
	 * {@code odilon.properties} (default: {@code 0}).
	 * </p>
	 */
	@Value("${volume.active:0}")
	protected int activeVolumeId;

	private DataStorage dataStorage;

	// LOCK SERVICE ------------------------------------------
	//
	//
	@Value("${lockRateMillisecs:2}")
	String s_lockRateMillisecs;
	protected double lockRateMillisecs;

	// SCHEDULER -------------------------------------------
	//
	@Value("${scheduler.standard.threads:0}")
	protected int schedulerThreads;
	
	
	@Value("${scheduler.standByReplica.threads:2}")
	protected int schedulerStandByReplicaThreads;
	
	public int getStandByReplicaDispatcherPoolSize() {
		return schedulerStandByReplicaThreads;
	}

	@Value("${scheduler.cron.threads:0}")
	protected int cronSchedulerThreads;

	@Value("${scheduler.siestaSecs:20}")
	protected long schedulerSiestaSecs;

	@Value("${scheduler.cronJobWorkDirCleanUp:15 5 * * * *}")
	protected String CronJobWorkDirCleanUp;

	// INTEGRITY CHECK -----------------------------------

	@Value("${integrityCheck:true}")
	protected boolean integrityCheck;

	@Value("${integrityCheckThreads:0}")
	protected int integrityCheckThreads;

	@Value("${integrityCheckDays:180}")
	protected int integrityCheckDays;

	@Value("${integrityCheckCronExpression:15 15 5 * * *}")
	protected String integrityCheckCronExpression;

	// VAULT -------------------------------------------

	@Value("${vault.enabled:false}")
	protected boolean vaultEnabled;

	@Value("${vault.newfiles:true}")
	protected boolean isVaultNewFiles;

	@Value("${vault.url:#{null}}")
	protected String vaultUrl;

	@Value("${vault.roleId:#{null}}")
	protected String vaultRoleId;

	@Value("${vault.secretId:#{null}}")
	protected String vaultSecretId;

	@Value("${vault.keyId:kbee-key}")
	protected String vaultKeyId;

	private Optional<String> o_vaultUrl;

	// STAND BY ------------------------------------------

	@Value("${standby.enabled:false}")
	protected boolean isStandByEnabled = false;

	@Value("${standby.sync.force:false}")
	protected boolean standbySyncForce = false;

	@Value("${standby.sync.threads:-1}")
	protected int standbySyncThreads = -1;

	/** Maximum number of pending replica queue entries before new writes are rejected (Issue 1) */
	@Value("${standby.replicaQueueMax:10000}")
	protected int standbyReplicaQueueMax;

	/** Milliseconds replicate() will wait for the journal commit to complete before timing out (Issue 2) */
	@Value("${standby.journalWaitMs:15000}")
	protected int standbyJournalWaitMs;

	/** Maximum consecutive errors before initial sync aborts and requires a restart (Issue 6) */
	@Value("${standby.sync.maxErrors:10}")
	protected int standbySyncMaxErrors;

	/** When true, initial sync propagates all object versions; when false (default) head only (Issue 5) */
	@Value("${standby.syncVersions:false}")
	protected boolean standbySyncVersions;

	@Value("${standby.url:null}")
	protected String standbyUrl;

	@Value("${standby.port:9234}")
	int standbyPort;

	@Value("${standby.accessKey:odilon}")
	protected String standbyAccessKey;

	@Value("${standby.secretKey:odilon}")
	protected String standbySecretKey;

	// TRAFFIC PASS --------------------------------------

	@Value("${traffic.tokens:32}")
	private String tokensStr;

	private int t_tokens;

	// OBJECT CACHES --------------------------------------

	@Value("${objectMetadataCache.initialCapacity:10000}")
	protected int objectCacheInitialCapacity;

	@Value("${objectMetadataCache.enabled:true}")
	protected boolean useObjectCache;

	@Value("${objectMetadataCache.maxCapacity:2000000}")
	protected long objectCacheMaxCapacity;

	@Value("${objectMetadataCache.expireDays:15}")
	protected long objectExpireDays;

	// FILE CACHE (USED BY RAID 6) -----------------------

	@Value("${fileCache.maxCapacity:100000}")
	protected long fileCacheMaxCapacity;

	@Value("${fileCache.durationDays:15}")
	protected int fileCacheDurationDays;

	@Value("${fileCache.initialCapacity:10000}")
	protected int fileCacheIntialCapacity;

	// RAID 6 BUFFERS (USED BY RAID 6) -----------------------

	@Value("${raid6.buffers:null}")
	private String raidSixBuffersStr;

	private int raidSixBuffers;

	/**
	 * When {@code true}, every RAID-6 read whose shards are all physically present
	 * will have their parity checked via {@link ReedSolomon#isParityCorrect} to
	 * detect silent byte-level corruption.  Repair is attempted for up to 2
	 * simultaneously corrupt shards (full N=6 tolerance).
	 * <p>Default: {@code false} — the check adds roughly one parity-encode pass
	 * per chunk of I/O and should only be enabled when active corruption detection
	 * on the read path is required.</p>
	 */
	@Value("${raid6.readParityCheck:false}")
	private boolean raid6ReadParityCheck;

	// --------------------------------------------------

	@Value("${retryFailedSeconds:20}")
	protected long retryFailedSeconds;

	private int totalDisks;

	@Autowired
	public ServerSettings() {
	}

	public OffsetDateTime getSystemStartTime() {
		return systemStarted;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public List<String> getRootDirs() {
		return rootDirs;
	}

	public boolean isEncryptionEnabled() {
		return isEncrypt;
	}

	public boolean isEncryptMetadata() {
		return isEncrypt && encryptMetadata;
	}

	@Override
	public String toJSON() {

		StringBuilder str = new StringBuilder();

		str.append("\"port\":\"" + String.valueOf(port) + "\"");
		str.append(", \"accessKey\":\"" + accessKey + "\"");
		str.append(", \"secretKey\":\"" + secretKey + "\"");

		str.append(", \"https\":\"" + (isHTTPS() ? "yes" : "no") + "\"");

		str.append(", \"Vault enabled\":\"" + "\"" + (isVaultEnabled() ? "true" : "false") + "\"");
		str.append(", \"Use Vault for new files\":\"" + "\"" + (isUseVaultNewFiles() ? "true" : "false") + "\"");
		str.append(", \"vaultUrl\":\"" + (Optional.ofNullable(vaultUrl).isPresent() ? ("\"" + vaultUrl + "\"") : "null"));
		str.append(", \"vaultKeyId\":\"" + (Optional.ofNullable(vaultKeyId).isPresent() ? ("\"" + vaultKeyId + "\"") : "null"));
		str.append(", \"vaultRoleId\":\"" + (Optional.ofNullable(vaultRoleId).isPresent() ? ("\"" + vaultRoleId + "\"") : "null"));

		str.append(", \"redundancyLevel\":" + (Optional.ofNullable(redundancyLevel).isPresent() ? ("\"" + redundancyLevel.getName() + "\"") : "null"));

		if (redundancyLevel == RedundancyLevel.RAID_6) {
			str.append(", \"dataDrives\":" + String.format("%3d", getRAID6DataDrives()).trim());
			str.append(", \"paritytDrives\":" + String.format("%3d", getRAID6ParityDrives()).trim());
			str.append(", \"R6BufferPoolSize\":" + String.format("%4d", this.getR6BufferPoolSize()).trim());
			str.append(", \"raid6.readParityCheck\":\"" + (isRAID6ReadParityCheckEnabled() ? "true" : "false") + "\"");
		}

		str.append(", \"dataDirs\":[");
		if (rootDirs != null && rootDirs.size() > 0)
			str.append(rootDirs.stream().map((s) -> "\"" + s + "\"").collect(Collectors.joining(", ")));
		str.append("]");
		
		
		

		// STAND BY --------------

		str.append(", \"standby.enabled\":\"" + "\"" + (isStandByEnabled() ? "true" : "false") + "\"");

		if (isStandByEnabled()) {
			str.append(", \"standby.url\":" + (Optional.ofNullable(standbyUrl).isPresent() ? ("\"" + standbyUrl + "\"") : "null"));
			str.append(", \"standby.accesskey\":" + (Optional.ofNullable(standbyUrl).isPresent() ? ("\"" + standbyAccessKey + "\"") : "null"));
			str.append(", \"standby.secretkey\":" + (Optional.ofNullable(standbySecretKey).isPresent() ? ("\"" + standbySecretKey + "\"") : "null"));
			str.append(", \"standby.port\":" + String.format("%6d", standbyPort).trim());
		}

		str.append("\"dataStorage\":\"" + getDataStorage() + "\"");

		str.append(", \"encrypt\":\"" + "\"" + (isEncryptionEnabled() ? "true" : "false") + "\"");

		str.append(", \"encryptMetadata\":\"" + "\"" + ( this.encryptMetadata ? "true" : "false") + "\"");

		str.append(", \"keyAlgorithm\":" + (Optional.ofNullable(keyAlgorithm).isPresent() ? ("\"" + keyAlgorithm + "\"") : "null"));

		str.append(", \"lockRateMillisecs\":" + String.format("%6.2f", getLockRateMillisecs()).trim());

		// Scheduler
		str.append("\"schedulerThreads\":\"" + String.valueOf(schedulerThreads) + "\"");
		str.append("\"schedulerSiestaSecs\":\"" + String.valueOf(schedulerSiestaSecs) + "\"");

		// Integrity Check
		str.append(", \"integrityCheck\":\"" + "\"" + (isIntegrityCheck() ? "true" : "false") + "\"");
		if (isIntegrityCheck()) {
			str.append(", \"integrityCheckThreads\":" + String.valueOf(getIntegrityCheckThreads()).trim());
			str.append(", \"integrityCheckDays\":" + String.valueOf(getIntegrityCheckDays()).trim());
			str.append(", \"integrityCheckCronExpression\":\"" + integrityCheckCronExpression + "\"");
		}

		str.append(", \"timeZone\":\"" + getTimeZone() + "\"");

		str.append(", \"trafficTokens\":" + String.valueOf(t_tokens) + "");
		str.append(", \"versionControl\":\"" + 	versionControlModel.getName() + "\"");
		
		
		str.append("\"objectMetadataCache.maxCapacity\":\"" + String.valueOf(objectCacheMaxCapacity) + "\"");
		str.append("\"objectMetadataCache.durationDays\":\"" + String.valueOf(objectExpireDays) + "\"");

		str.append("\"objectstream.cache.secs\":\"" + String.valueOf(getserverObjectstreamCacheSecs()) + "\"");

		str.append("\"fileCache.maxCapacity\":\"" + String.valueOf(fileCacheMaxCapacity) + "\"");
		str.append("\"fileCache.durationDays\":\"" + String.valueOf(fileCacheDurationDays) + "\"");

		return str.toString();
	}

	public int getRAID6ParityDrives() {
		return raid6ParityDrives;
	}

	public boolean isRAID6ReadParityCheckEnabled() {
		return raid6ReadParityCheck;
	}
	public int getRAID6DataDrives() {
		return raid6DataDrives;
	}

	/**
	 * Returns the number of RAID 6 volumes, derived at startup from the number of
	 * {@code dataStorage.volume.N} keys (new style) or {@code 1} for legacy
	 * {@code dataStorage=} deployments.
	 */
	public int getRAID6Volumes() {
		return raid6Volumes;
	}

	/**
	 * <p>
	 * Returns the ordered per-volume drive-directory lists.
	 * </p>
	 * <p>
	 * {@code get(v)} is the ordered list of root-directory paths for volume {@code v},
	 * matching the order in {@code dataStorage.volume.v=}. The position inside the
	 * list equals the <em>volume-local shard index</em> used in RS-encoded file names.
	 * </p>
	 * <p>
	 * For legacy {@code dataStorage=} deployments this returns a single-entry list
	 * (volume 0 only).
	 * </p>
	 */
	public List<List<String>> getVolumeRootDirs() {
		return volumeRootDirs;
	}

	/**
	 * Returns the 0-based id of the RAID 6 volume that currently receives new
	 * object writes. Controlled by {@code volume.active} in
	 * {@code odilon.properties} (default: {@code 0}).
	 */
	public int getActiveVolumeId() {
		return activeVolumeId;
	}

	public Map<String, Object> toMap() {

		Map<String, Object> map = new TreeMap<String, Object>();

		map.put("server.port", getPort());
		map.put("server.https", isHTTPS() ? "true" : "false");

		map.put("security.accessKey", accessKey);
		map.put("security.secretKey", secretKey.substring(0, 3) + "...");

		map.put("raid.redundancyLevel", Optional.ofNullable(redundancyLevel).isPresent() ? (redundancyLevel.getName()) : "null");
		int n = 0;
		if (rootDirs != null && rootDirs.size() > 0) {
			for (String s : rootDirs) {
				map.put("raid.rootDir_" + String.valueOf(n++), s);
			}
		}

		map.put("cache.serverObjectstreamCacheSecs", String.valueOf(getserverObjectstreamCacheSecs()));

		map.put("dataStorage.mode", getDataStorage().getName());

		map.put("standby.enabled", isStandByEnabled() ? "true" : "false");
		if (isStandByEnabled()) {
			map.put("standby.url", standbyUrl);
			map.put("standby.accesskey", standbyAccessKey);
			map.put("standby.secretkey", standbySecretKey.subSequence(0, 3) + "...");
			map.put("standby.port", String.format("%6d", standbyPort).trim());
		}

		map.put("timeZone", getTimeZone());

		return map;
	}

	public int getPort() {
		return port;
	}

	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(ServerSettings.class.getSimpleName() + "{");
		str.append(toJSON());
		str.append("}");
		return str.toString();
	}

	/**
	 * 
	 */
	@PostConstruct
	public void onInitialize() {

		if (this.secretKey == null) {
			exit("secretKey can not be null");
		}
		
		if (this.presignedSalt.length() < 20)
			exit("presignedSalt must be at least 20 characters (it has " + String.valueOf(this.presignedSalt.length()) + ")");

		if (this.presignedSalt.length() > 20)
			this.presignedSalt = this.presignedSalt.substring(0, 20);

		if (this.rootDirs == null || this.rootDirs.size() < 1) {
			startuplogger.error("No rootDirs are defined. \n" + "for RAID 0. at least 1 dataDir must be defined in file -> odilon.properties \n" + "for RAID 1. at least 1 dataDir must be defined in file -> odilon.properties \n"
					+ "for RAID 6. 3, 6, 12, 24 or 48 dataDirs must be defined in file -> odilon.properties \n" + "using default values ");

			getDefaultRootDirs().forEach(o -> startuplogger.error(o));
			this.rootDirs = getDefaultRootDirs();
		}

		
		
			
		if (this.versionControlStr==null || this.versionControlStr.toLowerCase().trim().equals("null")) {
			if (this.serverVersionControlStr!=null && !this.serverVersionControlStr.toLowerCase().trim().equals("null")) {
				this.versionControlStr = this.serverVersionControlStr.toLowerCase().trim();
			}
			else
				this.versionControlStr = "disabled";
		}
		
		
		if (this.versionControlStr==null)
				this.versionControlStr = "disabled";
		
		this.versionControlStr = this.versionControlStr.trim().toLowerCase();
				
		if (this.versionControlStr.equals("disabled") || this.versionControlStr.equals("false"))
			this.versionControlModel = VersionControl.DISABLED;
		
		else if (this.versionControlStr.equals("standard")|| this.versionControlStr.equals("true"))
			this.versionControlModel = VersionControl.STANDARD;
		
		else if (this.versionControlStr.equals("protected"))
			this.versionControlModel = VersionControl.PROTECTED;
		else
			throw new IllegalArgumentException("versionControl must be one of {disabled, standard, protected} -> provided value -> " + versionControlStr);
		
		
		if (this.encryptionKeyIV != null) {
			this.encryptionKeyIV = encryptionKeyIV.trim();

			// Legacy 128-bit key: 32 hex (key) + 24 hex (IV) = 56 chars
			// New     256-bit key: 64 hex (key) + 24 hex (IV) = 88 chars
			final int ivHexLen   = 2 * EncryptionService.AES_IV_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE; // always 24
			final int key128Len  = 2 * 128 / VirtualFileSystemService.BITS_PER_BYTE; // 32
			final int key256Len  = 2 * 256 / VirtualFileSystemService.BITS_PER_BYTE; // 64
			final int total128   = key128Len + ivHexLen; // 56
			final int total256   = key256Len + ivHexLen; // 88

			int totalLen = this.encryptionKeyIV.length();
			if (totalLen != total128 && totalLen != total256)
				exit("encryption.key length must be " + total128 + " hex chars (128-bit key) or "
						+ total256 + " hex chars (256-bit key) -> provided length: " + totalLen);
			try {
				@SuppressWarnings("unused")
				byte[] be = ByteToString.hexStringToByte(encryptionKeyIV);
			} catch (Exception e) {
				exit("encryption key is not a valid hex String -> " + encryptionKeyIV);
			}

			int keyHexLen = totalLen - ivHexLen;
			this.encryptionKey = encryptionKeyIV.substring(0, keyHexLen);
			this.encryptionIV  = encryptionKeyIV.substring(keyHexLen);
		}

		if ((this.masterKey == null) && (this.masterkey_lowercase != null)) {
			this.masterKey = this.masterkey_lowercase.trim();
		}

		if (masterKey != null) {
			masterKey = masterKey.trim();
			final int masterKey128Len = 2 * 128 / VirtualFileSystemService.BITS_PER_BYTE; // 32 hex chars
			final int masterKey256Len = 2 * 256 / VirtualFileSystemService.BITS_PER_BYTE; // 64 hex chars
			if (masterKey.length() != masterKey128Len && masterKey.length() != masterKey256Len)
				exit("masterKey length must be " + masterKey128Len + " hex chars (128-bit) or "
						+ masterKey256Len + " hex chars (256-bit) -> provided length: " + masterKey.length());
			try {
				@SuppressWarnings("unused")
				byte[] be = ByteToString.hexStringToByte(masterKey);
			} catch (Exception e) {
				exit("masterKey key is not a valid hex String -> " + masterKey);
			}
		}

		Integer t;

		try {
			t = Integer.valueOf(tokensStr);

		} catch (Exception e) {
			t = Integer.valueOf(ServerConstant.TRAFFIC_TOKENS_DEFAULT);
		}

		t_tokens = t.intValue();

		try {
			dataStorage = (dataStorageMode == null) ? DataStorage.READ_WRITE : DataStorage.fromValue(dataStorageMode);
		} catch (Exception e) {
			exit("dataStorage must be one of {" + DataStorage.getNames().toString() + "} -> " + dataStorageMode);
		}

		if (timeZone == null || timeZone.equals("null") || timeZone.length() == 0)
			timeZone = TimeZone.getDefault().getID();

		if (getTimeZone().equals(TimeZone.getDefault().getID()))
			TimeZone.setDefault(TimeZone.getTimeZone(getTimeZone()));

		if ((this.serverMode == null))
			this.serverMode = ServerConstant.MASTER_MODE;
		else {
			this.serverMode = this.serverMode.toLowerCase().trim();
			if (!(this.serverMode.equals(ServerConstant.MASTER_MODE) || serverMode.equals(ServerConstant.STANDBY_MODE)))
				exit("server.mode must be '" + ServerConstant.MASTER_MODE + "' or '" + ServerConstant.STANDBY_MODE + "' -> " + serverMode);
		}

		if (this.redundancyLevelStr == null || this.redundancyLevelStr.toLowerCase().trim().equals("null")) {
			exit("redundancyLevel can not be null | Supported values are: RAID 0, RAID 1 or RAID 6 | example -> redundancyLevel=RAID 0");
		}

		this.redundancyLevel = RedundancyLevel.get(redundancyLevelStr);

		// Normalise flat rootDirs for RAID 0, RAID 1 and legacy RAID 6.
		// For new-style RAID 6 (dataStorage.volume.N=) rootDirs starts empty;
		// the RAID 6 block below will build it from volumeRootDirs.
		boolean isNewStyleRAID6 = (this.redundancyLevel == RedundancyLevel.RAID_6)
				&& environment.containsProperty("dataStorage.volume.0");

		if (!isNewStyleRAID6) {
			List<String> dirs = new ArrayList<String>();
			this.rootDirs.forEach(item -> dirs.add(
					item.replace("/", File.separator)
					    .replace("\\", File.separator)
					    .replaceAll("[?;<>|]", "")
					    .trim()));
			List<String> unique = dirs.stream().filter(s -> !s.isEmpty()).distinct().collect(Collectors.toList());
			if (unique.size() != dirs.stream().filter(s -> !s.isEmpty()).count())
				exit("DataStorage can not have duplicate entries -> " + dirs.toString());
			this.rootDirs = unique;
		}

		this.o_vaultUrl = Optional.ofNullable(this.vaultUrl);

		if (this.redundancyLevel == null) {
			StringBuilder str = new StringBuilder();
			RedundancyLevel.getValues().forEach(item -> str.append((str.length() > 0 ? ", " : "") + item.getName()));
			exit("RedundancyLevel error -> " + redundancyLevelStr + " | Supported values are: " + str.toString());
		}

		if (this.redundancyLevel == RedundancyLevel.RAID_0) {
			if (this.rootDirs.size() < 1)
				exit("DataStorage must have at least 1 entry for -> " + redundancyLevel.getName() + " | dataStorage=" + rootDirs.toString());

		} else if (this.redundancyLevel == RedundancyLevel.RAID_1) {
			if (this.rootDirs.size() < 2)
				exit("DataStorage must have at least 2 entries for -> " + redundancyLevel.getName() + " | dataStorage=" + rootDirs.toString() + " | you must use " + RedundancyLevel.RAID_0.getName() + " for only one mount directory");

		} else if (this.redundancyLevel == RedundancyLevel.RAID_6) {

			// ── Detect configuration style ────────────────────────────────────────────
			//
			// NEW style:  dataStorage.volume.0=dir1,dir2,dir3
			//             dataStorage.volume.1=dir4,dir5,dir6
			//             volume.active=1
			//             (raid6.dataDrives and raid6.parityDrives are NOT needed)
			//
			// LEGACY fallback:  dataStorage=dir1,dir2,...,dirN
			//                   (single volume, backward compatible)

			boolean newStyle = environment.containsProperty("dataStorage.volume.0");

			if (newStyle) {
				// ── New per-volume style ───────────────────────────────────────────────
				this.volumeRootDirs = new ArrayList<>();
				int v = 0;
				while (environment.containsProperty("dataStorage.volume." + v)) {
					String raw = environment.getProperty("dataStorage.volume." + v, "");
					List<String> vDirs = new ArrayList<>();
					for (String d : raw.split(",")) {
						String trimmed = d.replace("/", File.separator)
								          .replace("\\", File.separator)
								          .replaceAll("[?;<>|]", "")
								          .trim();
						if (!trimmed.isEmpty())
							vDirs.add(trimmed);
					}
					if (vDirs.isEmpty())
						exit("dataStorage.volume." + v + " has no drive entries.");

					// All volumes must have the same drive count
					if (v > 0 && vDirs.size() != this.volumeRootDirs.get(0).size())
						exit("dataStorage.volume." + v + " has " + vDirs.size()
								+ " drives but volume 0 has " + this.volumeRootDirs.get(0).size()
								+ ". All volumes must have the same number of drives.");

					this.volumeRootDirs.add(vDirs);
					v++;
				}

				if (this.volumeRootDirs.isEmpty())
					exit("No dataStorage.volume.N entries found despite dataStorage.volume.0 being present.");

				int drivesPerVol = this.volumeRootDirs.get(0).size();
				this.raid6Volumes = this.volumeRootDirs.size();

				// Derive data/parity from drive count — no user config needed
				try {
					RAID6Config cfg = RAID6Config.fromDriveCount(drivesPerVol);
					this.raid6DataDrives   = cfg.dataDrives;
					this.raid6ParityDrives = cfg.parityDrives;
					startuplogger.info("RAID 6: " + this.raid6Volumes + " volume(s), "
							+ drivesPerVol + " drives/volume -> " + cfg);
				} catch (IllegalArgumentException e) {
					exit("dataStorage.volume.0 has " + drivesPerVol + " drives. "
							+ "Supported drives per volume: " + RAID6Config.supportedCounts() + ".");
				}

				// Build flat rootDirs from the per-volume lists (used by loadDrives)
				this.rootDirs = new ArrayList<>();
				for (List<String> vd : this.volumeRootDirs)
					this.rootDirs.addAll(vd);

			} else {
				// ── Legacy flat dataStorage style ─────────────────────────────────────
				// Single volume only.  Emit a deprecation warning.
				startuplogger.warn("RAID 6: using legacy 'dataStorage=' syntax (single volume). "
						+ "For multi-volume deployments use 'dataStorage.volume.N=' instead.");

				// Remove empty entries that result from the default empty split
				this.rootDirs = this.rootDirs.stream()
						.map(s -> s.replace("/", File.separator)
								   .replace("\\", File.separator)
								   .replaceAll("[?;<>|]", "")
								   .trim())
						.filter(s -> !s.isEmpty())
						.distinct()
						.collect(Collectors.toList());

				if (this.rootDirs.isEmpty())
					exit("redundancyLevel=RAID 6 requires at least one drive in 'dataStorage' or 'dataStorage.volume.0'.");

				int drivesPerVol = this.rootDirs.size();
				this.raid6Volumes = 1;

				try {
					RAID6Config cfg = RAID6Config.fromDriveCount(drivesPerVol);
					this.raid6DataDrives   = cfg.dataDrives;
					this.raid6ParityDrives = cfg.parityDrives;
				} catch (IllegalArgumentException e) {
					exit("dataStorage has " + drivesPerVol + " drives. "
							+ "Supported drives per volume: " + RAID6Config.supportedCounts() + ".");
				}

				// Wrap in single-volume list for uniform access
				this.volumeRootDirs = new ArrayList<>();
				this.volumeRootDirs.add(new ArrayList<>(this.rootDirs));
			}

			// ── Validate volume.active is in range ────────────────────────────────────
			// By design, only ONE volume can be active at a time (activeVolumeId is a single integer).
			// This validation ensures the configured active volume ID is within the valid range.
			if (this.activeVolumeId < 0 || this.activeVolumeId >= this.raid6Volumes) {
				exit("volume.active=" + this.activeVolumeId
						+ " is out of range. Total volumes=" + this.raid6Volumes
						+ " (valid range: 0.." + (this.raid6Volumes - 1) + ")");
			}
		}
		try {

			this.lockRateMillisecs = Double.valueOf(this.s_lockRateMillisecs).doubleValue();

			if (this.lockRateMillisecs < 0.01)
				this.lockRateMillisecs = 0.01;

			if (this.lockRateMillisecs > 10.0)
				this.lockRateMillisecs = 10;

		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);
			this.lockRateMillisecs = 2;
		}

		if (this.integrityCheckDays < 1)
			this.integrityCheckDays = 180;

		if (this.integrityCheckThreads < 1)
			this.integrityCheckThreads = Double.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors() - 1) / 2.0).intValue() + 1;

		if (this.schedulerThreads < 1)
			this.schedulerThreads = Double.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors() - 1) / 2.0).intValue() + 1;

		if (this.schedulerThreads < 2)
			this.schedulerThreads = 2;

		if (this.cronSchedulerThreads < 1)
			this.cronSchedulerThreads = Double.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors() - 1) / 2.5).intValue() + 1;

		if (this.cronSchedulerThreads < 2)
			this.cronSchedulerThreads = 2;

		if (this.standbyUrl == null)
			this.isStandByEnabled = false;

		if (fileCacheDurationDays < 1)
			fileCacheDurationDays = 7;

		if (this.objectCacheInitialCapacity < 1)
			this.objectCacheInitialCapacity = 10000;

		this.totalDisks = getRootDirs().size();

		if (this.redundancyLevel == RedundancyLevel.RAID_6) {
			try {
				if (raidSixBuffersStr != null && raidSixBuffersStr.equals("null")) {
					raidSixBuffers = ServerConstant.R6_BUFFERS;
					if (raidSixBuffers < this.rootDirs.size())
						raidSixBuffers = this.rootDirs.size();
				} else {
					raidSixBuffers = Integer.valueOf(raidSixBuffersStr).intValue();
				}
			} catch (Exception e) {
				raidSixBuffers = ServerConstant.R6_BUFFERS;
			}

			if (this.raidSixBuffers < this.rootDirs.size()) {
				startuplogger.info(" buffers is set to " + this.raidSixBuffers + "but they must be at least " + this.rootDirs.size() + ". We will use corrected the value to this value.");
				this.raidSixBuffers = this.rootDirs.size();
			}

		} else {
			raidSixBuffers = ServerConstant.R6_BUFFERS;
		}

		this.encryptMetadata = (isEncrypt && (encryptMetadataStr == null || encryptMetadataStr.trim().toLowerCase().equals("null") || encryptMetadataStr.trim().toLowerCase().equals("true"))) ? true : false;

		
		
		startuplogger.debug("Started -> " + ServerSettings.class.getSimpleName());

	}

	public boolean isReadOnly() {
		return getDataStorage() == DataStorage.READONLY;
	}

	public boolean isWORM() {
		return getDataStorage() == DataStorage.WORM;
	}

	public int getCronDispatcherPoolSize() {
		return cronSchedulerThreads;
	}

	public String getCronJobWorkDirCleanUp() {
		return CronJobWorkDirCleanUp;
	}

	public boolean isUseObjectCache() {
		return useObjectCache;
	}

	public String getInternalMasterKeyEncryptor() {
		return masterKey;
	}

	public void setInternalMasterKeyEncryptor(String masterKey) {
		this.masterKey = masterKey;
	}

	public int getserverObjectstreamCacheSecs() {
		return this.serverObjectstreamCacheSecs;
	}

	public String getStandBySecretKey() {
		return standbySecretKey;
	}

	public String getStandByAccessKey() {
		return standbyAccessKey;
	}

	public String getStandByUrl() {
		return standbyUrl;
	}

	public boolean isStandByEnabled() {
		return isStandByEnabled;
	}

	public VersionControl getVersionControl() {
		return this.versionControlModel;
	}

	public boolean isHTTPS() {
		return this.ishttps != null && this.ishttps.toLowerCase().trim().equals("true");
	}

	private List<String> getDefaultRootDirs() {

		if (isWindows()) {

			/** Windows */

			List<String> list = new ArrayList<String>();

			if (getRedundancyLevel() == RedundancyLevel.RAID_1 || getRedundancyLevel() == RedundancyLevel.RAID_0) {
				list.add("c:" + File.separator + "odilon-data" + File.separator + "drive0");
				return list;
			}

			/** for RAID 6 default is 3,1 */
			list.add("c:" + File.separator + "odilon-data" + File.separator + "drive0");
			list.add("c:" + File.separator + "odilon-data" + File.separator + "drive1");
			list.add("c:" + File.separator + "odilon-data" + File.separator + "drive2");
			return list;
		}

		{
			/** Linux or mac */

			List<String> list = new ArrayList<String>();

			if (getRedundancyLevel() == RedundancyLevel.RAID_1 || getRedundancyLevel() == RedundancyLevel.RAID_0) {
				list.add(File.separator + "var" + File.separator + "lib" + File.separator + "odilon-data" + File.separator + "drive0");
				return list;
			}
			
			list.add(File.separator + "opt" + File.separator + "odilon-data" + File.separator + "drive0");
			list.add(File.separator + "opt" + File.separator + "odilon-data" + File.separator + "drive1");
			list.add(File.separator + "opt" + File.separator + "odilon-data" + File.separator + "drive2");
			
			return list;
		}
	}

	public String getKeyAlgorithm() {
		return keyAlgorithm;
	}

	public void setKeyAlgorithm(String keyAlgorithm) {
		this.keyAlgorithm = keyAlgorithm;
	}

	public String getVersion() {
		return version;
	}

	public double getLockRateMillisecs() {
		return lockRateMillisecs;
	}

	public RedundancyLevel getRedundancyLevel() {
		return redundancyLevel;
	}

	public String getRoleId() {
		return vaultRoleId;
	}

	public String getSecretId() {
		return vaultSecretId;
	}

	public Optional<String> getVaultUrl() {
		return o_vaultUrl;
	}

	public String getVaultKeyId() {
		return vaultKeyId;
	}

	public String getEncryptionKey() {
		return encryptionKey;
	}

	public String getEncryptionIV() {
		return encryptionIV;
	}

	/**
	 * <p>
	 * This method is used to define whether new files will use Vault or local key
	 * encryptor:<br/>
	 * <b> false </b> existing files encrypted with Vault will use it to decrypt,
	 * new file will not<br/>
	 * <b> true </b> if vault.url points to an existing Vault it will use it,
	 * otherwise it will not<br/>
	 * </p>
	 * 
	 * @return
	 */

	public void setRecovery(boolean isRecovery) {
		this.recovery = isRecovery;
	}

	public boolean isRecovery() {
		return recovery;
	}

	public boolean isVaultEnabled() {
		return vaultEnabled;
	}

	public boolean isUseVaultNewFiles() {
		return isVaultNewFiles;
	}

	public String[] getAppCharacterName() {
		return OdilonVersion.getAppCharacterName();
	}

	public boolean isIntegrityCheck() {
		return integrityCheck;
	}

	public int getDispatcherPoolSize() {
		return schedulerThreads;
	}

	public int getMaxTrafficTokens() {
		return t_tokens;
	}

	public int getIntegrityCheckThreads() {
		return integrityCheckThreads;
	}

	public int getIntegrityCheckDays() {
		return integrityCheckDays;
	}

	public String getIntegrityCheckCronExpression() {
		return integrityCheckCronExpression;
	}

	public long getSchedulerSiestaSecs() {
		return schedulerSiestaSecs;
	}

	public String getStandbyUrl() {
		return standbyUrl;
	}

	public void setStandbyUrl(String standbyUrl) {
		this.standbyUrl = standbyUrl;
	}

	public int getStandbyPort() {
		return standbyPort;
	}

	public void setStandbyPort(int standbyPort) {
		this.standbyPort = standbyPort;
	}

	public String getStandbyAccessKey() {
		return standbyAccessKey;
	}

	public void setStandbyAccessKey(String standbyAccessKey) {
		this.standbyAccessKey = standbyAccessKey;
	}

	public String getStandbySecretKey() {
		return standbySecretKey;
	}

	public String getServerMode() {
		return serverMode;
	}

	public void setStandbySecretKey(String standbySecretKey) {
		this.standbySecretKey = standbySecretKey;
	}

	public void setStandBy(boolean isStandBy) {
		this.isStandByEnabled = isStandBy;
	}

	public boolean isStandbySyncForce() {
		return this.standbySyncForce;
	}

	public int getStandbySyncThreads() {
		return this.standbySyncThreads;
	}

	public int getStandbyReplicaQueueMax() {
		return this.standbyReplicaQueueMax;
	}

	public int getStandbyJournalWaitMs() {
		return this.standbyJournalWaitMs;
	}

	public int getStandbySyncMaxErrors() {
		return this.standbySyncMaxErrors;
	}

	public boolean isStandbySyncVersions() {
		return this.standbySyncVersions;
	}

	public int getFileCacheInitialCapacity() {
		return fileCacheIntialCapacity;
	}

	public long getRetryFailedSeconds() {
		return retryFailedSeconds;
	}

	public String getTimeZone() {
		return this.timeZone;
	}

	public int getObjectCacheInitialCapacity() {
		return objectCacheInitialCapacity;
	}

	public int getTotalDisks() {
		return totalDisks;
	}

	public synchronized OdilonServerInfo getDefaultOdilonServerInfo() {

		OffsetDateTime now = OffsetDateTime.now();

		OdilonServerInfo si = new OdilonServerInfo();
		si.setCreationDate(now);
		si.setName(ServerConstant.applicationName);

		
		// ----
		// TODO VER
		// si.setVersionControl(isVersionControl());
		// --
		
		
		si.setEncryptionIntialized(false);

		if (getVersionControl()!=VersionControl.DISABLED)
			si.setVersionControlDate(now);

		
		si.setServerMode(getServerMode());
		si.setId(randomString(16));
		si.setStandByEnabled(isStandByEnabled());
		si.setStandbyUrl(getStandbyUrl());
		si.setStandbyPort(getStandbyPort());
		si.setStandBySyncedDate(null);
		si.setRedundancyLevel(this.getRedundancyLevel().getName());

		if (isStandByEnabled())
			si.setStandByStartDate(now);

		return si;
	}

	public long getObjectCacheCapacity() {
		return this.objectCacheMaxCapacity;
	}

	public long getObjectCacheExpireDays() {
		return this.objectExpireDays;
	}

	public long getFileCacheMaxCapacity() {
		return this.fileCacheMaxCapacity;
	}

	public long getFileCacheDurationDays() {
		return this.fileCacheDurationDays;
	}

	public boolean isRAID6ConfigurationValid(int dataShards, int parityShards) {
		return (dataShards == 32 && parityShards == 16) || (dataShards == 16 && parityShards == 8) || (dataShards == 8 && parityShards == 4) || (dataShards == 4 && parityShards == 2) || (dataShards == 2 && parityShards == 1);
	}

	public DataStorage getDataStorage() {
		return this.dataStorage;
	}

	public boolean isPingEnabled() {
		return this.pingEnabled;
	}

	protected String randomString(final int size) {
		return idGenerator.randomString(size);
	}

	private void exit(String msg) {
		logger.error(ServerConstant.SEPARATOR);
		logger.error(msg);
		logger.error("check file ." + File.separator + "config" + File.separator + "odilon.properties");
		logger.error(ServerConstant.SEPARATOR);
		System.exit(1);
	}

	public String getPresignedSalt() {
		return this.presignedSalt;
	}

	public int getR6BufferPoolSize() {
		return raidSixBuffers;
	}

	public int getR6BufferSizeMB() {
		return ServerConstant.MAX_CHUNK_SIZE;
	}
	
	private boolean isWindows() {
		if ((System.getenv("OS") != null) && System.getenv("OS").toLowerCase().contains("windows"))
			return true;
		return false;
	}

	
}
