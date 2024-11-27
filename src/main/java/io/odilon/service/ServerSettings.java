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
package io.odilon.service;

import java.io.File;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;

import org.checkerframework.checker.index.qual.NonNegative;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.lang.NonNull;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.OdilonVersion;
import io.odilon.encryption.EncryptionService;
import io.odilon.log.Logger;
import io.odilon.model.JSONObject;
import io.odilon.model.DataStorage;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.RedundancyLevel;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
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

    public boolean isPingEnabled() {
        return this.pingEnabled;
    }

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
    @NonNull
    protected String accessKey;

    /* default -> odilon */
    @Value("${secretKey:odilon}")
    @NonNull
    protected String secretKey;

    /* default port -> 9234 */
    @Value("${server.port:9234}")
    protected int port;

    @Value("${timezone:null}")
    protected String timeZone;

    public String getTimeZone() {
        return this.timeZone;
    }

    @Value("${server.mode:master}") /** server.mode = master | standby */
    protected String serverMode;

    /** Version Control. by default not enabled **/
    @Value("${server.versioncontrol:false}")
    protected boolean versioncontrol;

    @Value("${recoveryMode:false}")
    protected boolean isRecoverMode = false;

    @Value("${server.ssl.enabled}")
    protected String ishttps;

    // ENCRYPTION -----------------------------------------------
    //
    // by default encryption is not enabled
    //
    @Value("${encryption.enabled:false}")
    protected boolean isEncrypt;

    @Value("${encryption.key:#{null}}")
    protected String encryptionKeyIV;

    @Value("${encryption.masterkey:#{null}}")
    protected String masterkey_lowercase;

    @Value("${encryption.masterKey:#{null}}")
    protected String masterKey;

    @Value("${encryption.keyAlgorithm:AES}")
    protected String keyAlgorithm;

    // DATA STORAGE -------------------------------------------
    // by default RAID 0 with 1 directory
    //
    //
    @Value("${redundancyLevel:RAID 0}")
    @NonNull
    protected String redundancyLevelStr;

    @Value("#{'${dataStorage}'.split(',')}")
    @NonNull
    private List<String> rootDirs;

    @Value("${raid6.dataDrives:-1}")
    protected int raid6DataDrives;

    @Value("${raid6.parityDrives:-1}")
    protected int raid6ParityDrives;

    @Value("${dataStorageMode:rw}")
    @NonNull
    protected String dataStorageMode;
    /** readwrite, readonly, WORM */

    private DataStorage dataStorage;

    // LOCK SERVICE ------------------------------------------
    //
    @Value("${lockRateMillisecs:2}")
    String s_lockRateMillisecs;
    protected double lockRateMillisecs;

    // SCHEDULER -------------------------------------------
    //
    @Value("${scheduler.standard.threads:0}")
    protected int schedulerThreads;

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

    @Value("${standby.url:null}")
    protected String standbyUrl;

    @Value("${standby.port:9234}")
    int standbyPort;

    @Value("${standby.accessKey:odilon}")
    protected String standbyAccessKey;

    @Value("${standby.secretKey:odilon}")
    protected String standbySecretKey;

    // TRAFFIC PASS --------------------------------------

    @JsonProperty("traffic.tokens:12")
    private int tokens;

    // OBJECT CACHES --------------------------------------

    @Value("${objectMetadataCacheInitialCapacity:10000}")
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

    // --------------------------------------------------

    @Value("${retryFailedSeconds:20}")
    protected long retryFailedSeconds;

    /**
     * 
     * 
     */
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

    @Override
    public String toJSON() {

        StringBuilder str = new StringBuilder();

        str.append("\"port\":\"" + String.valueOf(port) + "\"");
        str.append(", \"accessKey\":\"" + accessKey + "\"");
        str.append(", \"secretKey\":\"" + secretKey + "\"");

        str.append(", \"https\":\"" + (isHTTPS() ? "yes" : "no") + "\"");

        str.append(", \"Vault enabled\":\"" + "\"" + (isVaultEnabled() ? "true" : "false") + "\"");
        str.append(", \"Use Vault for new files\":\"" + "\"" + (isUseVaultNewFiles() ? "true" : "false") + "\"");
        str.append(
                ", \"vaultUrl\":\"" + (Optional.ofNullable(vaultUrl).isPresent() ? ("\"" + vaultUrl + "\"") : "null"));
        str.append(", \"vaultKeyId\":\""
                + (Optional.ofNullable(vaultKeyId).isPresent() ? ("\"" + vaultKeyId + "\"") : "null"));
        str.append(", \"vaultRoleId\":\""
                + (Optional.ofNullable(vaultRoleId).isPresent() ? ("\"" + vaultRoleId + "\"") : "null"));

        str.append(", \"redundancyLevel\":"
                + (Optional.ofNullable(redundancyLevel).isPresent() ? ("\"" + redundancyLevel.getName() + "\"")
                        : "null"));

        if (redundancyLevel == RedundancyLevel.RAID_6) {
            str.append(", \"dataDrives\":" + String.format("%3d", getRAID6DataDrives()).trim());
            str.append(", \"paritytDrives\":" + String.format("%3d", getRAID6ParityDrives()).trim());
        }

        str.append(", \"dataDirs\":[");
        if (rootDirs != null && rootDirs.size() > 0)
            str.append(rootDirs.stream().map((s) -> "\"" + s + "\"").collect(Collectors.joining(", ")));
        str.append("]");

        // STAND BY --------------

        str.append(", \"standby.enabled\":\"" + "\"" + (isStandByEnabled() ? "true" : "false") + "\"");

        if (isStandByEnabled()) {
            str.append(", \"standby.url\":"
                    + (Optional.ofNullable(standbyUrl).isPresent() ? ("\"" + standbyUrl + "\"") : "null"));
            str.append(", \"standby.accesskey\":"
                    + (Optional.ofNullable(standbyUrl).isPresent() ? ("\"" + standbyAccessKey + "\"") : "null"));
            str.append(", \"standby.secretkey\":"
                    + (Optional.ofNullable(standbySecretKey).isPresent() ? ("\"" + standbySecretKey + "\"") : "null"));
            str.append(", \"standby.port\":" + String.format("%6d", standbyPort).trim());
        }

        str.append("\"dataStorage\":\"" + getDataStorage() + "\"");

        str.append(", \"encrypt\":\"" + "\"" + (isEncryptionEnabled() ? "true" : "false") + "\"");
        str.append(", \"keyAlgorithm\":"
                + (Optional.ofNullable(keyAlgorithm).isPresent() ? ("\"" + keyAlgorithm + "\"") : "null"));

        // str.append(", \"encryptionAlgorithm\":" +
        // (Optional.ofNullable(encryptionAlgorithm).isPresent() ?
        // ("\""+encryptionAlgorithm+"\"") :"null"));
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

        str.append(", \"trafficTokens\":" + String.valueOf(tokens) + "");
        str.append(", \"versionControl\":\"" + (this.versioncontrol ? "true" : "false") + "\"");

        str.append("\"objectMetadataCache.maxCapacity\":\"" + String.valueOf(objectCacheMaxCapacity) + "\"");
        str.append("\"objectMetadataCache.durationDays\":\"" + String.valueOf(objectExpireDays) + "\"");

        str.append("\"fileCache.maxCapacity\":\"" + String.valueOf(fileCacheMaxCapacity) + "\"");
        str.append("\"fileCache.durationDays\":\"" + String.valueOf(fileCacheDurationDays) + "\"");

        return str.toString();
    }

    public int getRAID6ParityDrives() {
        return raid6ParityDrives;
    }

    public int getRAID6DataDrives() {
        return raid6DataDrives;
    }

    public Map<String, Object> toMap() {

        Map<String, Object> map = new HashMap<String, Object>();

        map.put("port", getPort());
        map.put("accessKey", accessKey);
        map.put("secretKey", secretKey);
        map.put("redundancyLevel",
                Optional.ofNullable(redundancyLevel).isPresent() ? (redundancyLevel.getName()) : "null");
        int n = 0;
        if (rootDirs != null && rootDirs.size() > 0) {
            for (String s : rootDirs) {
                map.put("rootDir_" + String.valueOf(n++), s);
            }
        }

        map.put("dataStorage", getDataStorage().getName());

        map.put("https", isHTTPS() ? "true" : "false");

        map.put("keyAlgorithm", (Optional.ofNullable(keyAlgorithm).isPresent() ? (keyAlgorithm) : "null"));
        // map.put("encryptionAlgorithm",
        // (Optional.ofNullable(encryptionAlgorithm).isPresent() ? (encryptionAlgorithm)
        // :"null"));
        map.put("lockRateMillisecs", String.format("%6.2f", getLockRateMillisecs()).trim());

        map.put("standby.enabled", isStandByEnabled() ? "true" : "false");

        if (isStandByEnabled()) {
            map.put("standby.url", standbyUrl);
            map.put("standby.accesskey", standbyAccessKey);
            map.put("standby.secretkey", standbySecretKey);
            map.put("standby.port", String.format("%6d", standbyPort).trim());
        }

        map.put("timeZone", getTimeZone());

        return map;
    }

    /**
     * 
     * 
     */
    public int getPort() {
        return port;
    }

    /**
     * 
     * 
     */
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

        if (this.rootDirs == null || this.rootDirs.size() < 1) {
            startuplogger.error("No rootDirs are defined. \n"
                    + "for RAID 0. at least 1 dataDir must be defined in file -> odilon.properties \n"
                    + "for RAID 1. at least 1 dataDir must be defined in file -> odilon.properties \n"
                    + "for RAID 6. 3, 6, 12, 24 or 48 dataDirs must be defined in file -> odilon.properties \n"
                    + "using default values ");

            getDefaultRootDirs().forEach(o -> startuplogger.error(o));
            this.rootDirs = getDefaultRootDirs();
        }

        if (this.encryptionKeyIV != null) {
            this.encryptionKeyIV = encryptionKeyIV.trim();
            if (this.encryptionKeyIV
                    .length() != (2 * (EncryptionService.AES_KEY_SIZE_BITS + EncryptionService.AES_IV_SIZE_BITS)
                            / VirtualFileSystemService.BITS_PER_BYTE))
                exit("encryption key length must be -> "
                        + String.valueOf((2 * (EncryptionService.AES_KEY_SIZE_BITS + EncryptionService.AES_IV_SIZE_BITS)
                                / VirtualFileSystemService.BITS_PER_BYTE)));
            try {
                @SuppressWarnings("unused")
                byte[] be = ByteToString.hexStringToByte(encryptionKeyIV);
            } catch (Exception e) {
                exit("encryption key is not a valid hex String -> " + encryptionKeyIV);
            }

            this.encryptionKey = encryptionKeyIV.substring(0,
                    2 * EncryptionService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE);
            this.encryptionIV = encryptionKeyIV
                    .substring(2 * EncryptionService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE);

        }

        if ((this.masterKey == null) && (this.masterkey_lowercase != null)) {
            this.masterKey = this.masterkey_lowercase.trim();
        }

        if (masterKey != null) {
            masterKey = masterKey.trim();
            if (masterKey
                    .length() != (2 * EncryptionService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE))
                exit("masterKey key length must be -> " + String
                        .valueOf((2 * EncryptionService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE)));
            try {
                @SuppressWarnings("unused")
                byte[] be = ByteToString.hexStringToByte(masterKey);
            } catch (Exception e) {
                exit("masterKey key is not a valid hex String -> " + masterKey);
            }
        }

        if (tokens < 1)
            tokens = ServerConstant.TRAFFIC_TOKENS_DEFAULT;
        try {
            dataStorage = (dataStorageMode == null) ? DataStorage.READ_WRITE : DataStorage.fromString(dataStorageMode);
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
                exit("server.mode must be '" + ServerConstant.MASTER_MODE + "' or '" + ServerConstant.STANDBY_MODE
                        + "' -> " + serverMode);
        }

        if (this.redundancyLevelStr == null)
            this.redundancyLevel = RedundancyLevel.RAID_0;

        this.redundancyLevel = RedundancyLevel.get(redundancyLevelStr);

        List<String> dirs = new ArrayList<String>();

        if (isWindows())
            this.rootDirs.forEach(item -> dirs.add(
                    item.replace("/", File.separator).replace("\\", File.separator).replaceAll("[?;<>|]", "").trim()));
        else
            this.rootDirs.forEach(item -> dirs.add(
                    item.replace("/", File.separator).replace("\\", File.separator).replaceAll("[?;<>|]", "").trim()));

        this.rootDirs = dirs.stream().distinct().collect(Collectors.toList());

        if (this.rootDirs.size() != dirs.size())
            exit("DataStorage can not have duplicate entries -> " + dirs.toString());

        this.rootDirs = dirs;

        this.o_vaultUrl = Optional.ofNullable(this.vaultUrl);

        if (this.redundancyLevel == null) {
            StringBuilder str = new StringBuilder();
            RedundancyLevel.getValues().forEach(item -> str.append((str.length() > 0 ? ", " : "") + item.getName()));
            exit("RedundancyLevel error -> " + redundancyLevelStr + " | Supported values are: " + str.toString());
        }

        if (this.redundancyLevel == RedundancyLevel.RAID_1) {
            if (this.rootDirs.size() <= 1)
                exit("DataStorage must have at least 2 entries for -> " + redundancyLevel.getName() + " | dataStorage="
                        + rootDirs.toString() + " | you must use " + RedundancyLevel.RAID_0.getName()
                        + "for only one mount directory");
        } else if (this.redundancyLevel == RedundancyLevel.RAID_6) {

            if (!((this.rootDirs.size() == 3) || (this.rootDirs.size() == 6) || (this.rootDirs.size() == 12)
                    || (this.rootDirs.size() == 24) || (this.rootDirs.size() == 48))) {
                exit("DataStorage must have 3, 6, 12, 24, 48 entries for -> " + redundancyLevel.getName()
                        + " | value provided -> " + String.valueOf(this.rootDirs.size()));
            }

            if (this.rootDirs.size() == 3) {
                if (this.raid6DataDrives == -1)
                    this.raid6DataDrives = 2;
                if (this.raid6ParityDrives == -1)
                    this.raid6ParityDrives = 1;
            } else if (this.rootDirs.size() == 6) {
                if (this.raid6DataDrives == -1)
                    this.raid6DataDrives = 4;
                if (this.raid6ParityDrives == -1)
                    this.raid6ParityDrives = 2;
            } else if (this.rootDirs.size() == 12) {
                if (this.raid6DataDrives == -1)
                    this.raid6DataDrives = 8;
                if (this.raid6ParityDrives == -1)
                    this.raid6ParityDrives = 4;
            } else if (this.rootDirs.size() == 24) {
                if (this.raid6DataDrives == -1)
                    this.raid6DataDrives = 16;
                if (this.raid6ParityDrives == -1)
                    this.raid6ParityDrives = 8;
            }

            else if (this.rootDirs.size() == 48) {
                if (this.raid6DataDrives == -1)
                    this.raid6DataDrives = 32;
                if (this.raid6ParityDrives == -1)
                    this.raid6ParityDrives = 16;
            }

            if (!(((this.rootDirs.size() == 3) && (this.raid6DataDrives == 2) && (this.raid6ParityDrives == 1))
                    || ((this.rootDirs.size() == 6) && (this.raid6DataDrives == 4) && (this.raid6ParityDrives == 2))
                    || ((this.rootDirs.size() == 12) && (this.raid6DataDrives == 8) && (this.raid6ParityDrives == 4))
                    || ((this.rootDirs.size() == 24) && (this.raid6DataDrives == 16) && (this.raid6ParityDrives == 8))
                    || ((this.rootDirs.size() == 48) && (this.raid6DataDrives == 32)
                            && (this.raid6ParityDrives == 16)))) {
                exit(RedundancyLevel.RAID_6.getName() + " configurations supported are -> "
                        + " 3 dirs in DataStorage and raid6.dataDrives=2 and raid6.parityDrives=1  | "
                        + " 6 dirs in DataStorage and raid6.dataDrives=4 and raid6.parityDrives=2  | "
                        + "12 dirs in DataStorage and raid6.dataDrives=8 and raid6.parityDrives=4  | "
                        + "24 dirs in DataStorage and raid6.dataDrives=16 and raid6.parityDrives=8 |  "
                        + "48 dirs in DataStorage and raid6.dataDrives=32 and raid6.parityDrives=16 ");
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
            this.integrityCheckThreads = Double
                    .valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors() - 1) / 2.0).intValue() + 1;

        if (this.schedulerThreads < 1)
            this.schedulerThreads = Double.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors() - 1) / 2.0)
                    .intValue() + 1;

        if (this.schedulerThreads < 2)
            this.schedulerThreads = 2;

        if (this.cronSchedulerThreads < 1)
            this.cronSchedulerThreads = Double
                    .valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors() - 1) / 2.5).intValue() + 1;

        if (this.cronSchedulerThreads < 2)
            this.cronSchedulerThreads = 2;

        if (this.standbyUrl == null)
            this.isStandByEnabled = false;

        if (fileCacheMaxCapacity == -1) {

        }

        if (fileCacheDurationDays < 1)
            fileCacheDurationDays = 7;

        if (this.objectCacheInitialCapacity < 1)
            this.objectCacheInitialCapacity = 10000;

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

    public boolean isVersionControl() {
        return this.versioncontrol;
    }

    public boolean isHTTPS() {
        return this.ishttps != null && this.ishttps.toLowerCase().trim().equals("true");
    }

    private List<String> getDefaultRootDirs() {

        if (isWindows()) {

            /** Windows */

            List<String> list = new ArrayList<String>();
            list.add("c:" + File.separator + "odilon-data" + File.separator + "drive0");

            if (getRedundancyLevel() == RedundancyLevel.RAID_1 || getRedundancyLevel() == RedundancyLevel.RAID_0)
                return list;

            // for RAID 6 default is 3,1
            list.add("c:" + File.separator + "odilon-data" + File.separator + "drive0");
            list.add("c:" + File.separator + "odilon-data" + File.separator + "drive1");
            list.add("c:" + File.separator + "odilon-data" + File.separator + "drive2");
            return list;
        }

        {

            /** Linux */

            List<String> list = new ArrayList<String>();
            list.add(File.separator + "var" + File.separator + "lib" + File.separator + "odilon-data" + File.separator
                    + "drive0");

            if (getRedundancyLevel() == RedundancyLevel.RAID_1 || getRedundancyLevel() == RedundancyLevel.RAID_0)
                return list;

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

    public void setRecoveryMode(boolean isRecoveryMode) {
        this.isRecoverMode = isRecoveryMode;
    }

    public boolean isRecoverMode() {
        return isRecoverMode;
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
        return tokens;
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

    public @NonNegative int getFileCacheInitialCapacity() {
        return fileCacheIntialCapacity;
    }

    public @NonNegative long getRetryFailedSeconds() {
        return retryFailedSeconds;
    }

    /**
     * 
     */

    public synchronized OdilonServerInfo getDefaultOdilonServerInfo() {

        OffsetDateTime now = OffsetDateTime.now();

        OdilonServerInfo si = new OdilonServerInfo();
        si.setCreationDate(now);
        si.setName("Odilon");
        si.setVersionControl(isVersionControl());

        si.setEncryptionIntialized(false);

        if (isVersionControl())
            si.setVersionControlDate(now);

        si.setServerMode(getServerMode());
        si.setId(randomString(16));
        si.setStandByEnabled(isStandByEnabled());
        si.setStandbyUrl(getStandbyUrl());
        si.setStandbyPort(getStandbyPort());
        si.setStandBySyncedDate(null);

        if (isStandByEnabled())
            si.setStandByStartDate(now);

        return si;
    }

    private boolean isWindows() {
        if ((System.getenv("OS") != null) && System.getenv("OS").toLowerCase().contains("windows"))
            return true;
        return false;
    }

    public long getObjectCacheCapacity() {
        return this.objectCacheMaxCapacity;
    }

    public @NonNegative long getObjectCacheExpireDays() {
        return this.objectExpireDays;
    }

    public long getFileCacheMaxCapacity() {
        return this.fileCacheMaxCapacity;
    }

    public long getFileCacheDurationDays() {
        return this.fileCacheDurationDays;
    }

    public boolean isRAID6ConfigurationValid(int dataShards, int parityShards) {
        return (dataShards == 32 && parityShards == 16) || (dataShards == 16 && parityShards == 8)
                || (dataShards == 8 && parityShards == 4) || (dataShards == 4 && parityShards == 2)
                || (dataShards == 2 && parityShards == 1);
    }

    public DataStorage getDataStorage() {
        return this.dataStorage;
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

    public @NonNegative int getObjectCacheInitialCapacity() {
        return objectCacheInitialCapacity;
    }

}
