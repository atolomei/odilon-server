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
package io.odilon.virtualFileSystem.raid1;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.io.Files;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.SharedConstant;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.IODriveSetup;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/***
 * <p>
 * Set up new drives is <b>Async</b> for RAID 1.<br/>
 * This object will create and start a new {@link RAIDOneDriveSync} to duplicate
 * objects into the newly added drive/s in background
 * </p>
 * 
 * <p>
 * For RAID 1 this object starts an Async process {@link RAIDOneDriveSync} that
 * runs in background
 * </p>
 * 
 * @see {@link RaidOneDriveSync}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
public class RAIDOneDriveSetup implements IODriveSetup, ApplicationContextAware {

    static private Logger logger = Logger.getLogger(RAIDOneDriveSetup.class.getName());
    static private Logger startuplogger = Logger.getLogger("StartupLogger");

    @JsonIgnore
    private AtomicLong checkOk = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong counter = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong copied = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong totalBytes = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong errors = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong cleaned = new AtomicLong(0);

    @JsonIgnore
    private AtomicLong notAvailable = new AtomicLong(0);

    @JsonIgnore
    private RAIDOneDriver driver;

    @JsonIgnore
    private ApplicationContext applicationContext;

    public RAIDOneDriveSetup(RAIDOneDriver driver) {
        this.driver = driver;
    }

    @Override
    public boolean setup() {

        startuplogger.info("This process is non blocking for RAID 1");
        startuplogger.info("It will start a background process that will replicate all objects in the new drive/s.");

        final OdilonServerInfo serverInfo = getDriver().getServerInfo();
        final File keyFile = getDriver().getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
        final String jsonString;

        try {
            jsonString = getDriver().getObjectMapper().writeValueAsString(serverInfo);
        } catch (JsonProcessingException e) {
            startuplogger.error(e, SharedConstant.NOT_THROWN);
            return false;
        }

        try {
            startuplogger.info("1. Copying -> " + VirtualFileSystemService.SERVER_METADATA_FILE);
            getDriver().getDrivesAll().forEach(item -> {
                File file = item.getSysFile(VirtualFileSystemService.SERVER_METADATA_FILE);
                if ((item.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) && ((file == null) || (!file.exists()))) {
                    try {
                        item.putSysFile(VirtualFileSystemService.SERVER_METADATA_FILE, jsonString);
                    } catch (Exception e) {
                        startuplogger.error(e, "Drive -> " + item.getName());
                        throw new InternalCriticalException(e, "Drive -> " + item.getName());
                    }
                }
            });

            if ((keyFile != null) && keyFile.exists()) {
                startuplogger.info("2. Copying -> " + VirtualFileSystemService.ENCRYPTION_KEY_FILE);
                getDriver().getDrivesAll().forEach(item -> {
                    File file = item.getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
                    if ((item.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) && ((file == null) || (!file.exists()))) {
                        try {
                            Files.copy(keyFile, file);
                        } catch (Exception e) {
                            throw new InternalCriticalException(e, "Drive -> " + item.getName());
                        }
                    }
                });
            } else {
                startuplogger.info("2. Copying -> " + VirtualFileSystemService.ENCRYPTION_KEY_FILE + " | file not exist. skipping");
            }

        } catch (Exception e) {
            startuplogger.error(e, SharedConstant.NOT_THROWN);
            startuplogger.error("The process can not be completed due to errors");
            return false;
        }

        createBuckets();

        if (this.errors.get() > 0 || this.notAvailable.get() > 0) {
            startuplogger.error("The process can not be completed due to errors");
            return false;
        }

        startuplogger.info("4. Starting Async process -> " + RAIDOneDriveSync.class.getName());

        /** The rest of the process is async */
        @SuppressWarnings("unused")
        RAIDOneDriveSync checker = getApplicationContext().getBean(RAIDOneDriveSync.class, getDriver());

        /** sleeps 2 secs and return */
        try {
            Thread.sleep(1000 * 2);
        } catch (InterruptedException e) {
        }

        startuplogger.info("done");

        return true;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public ApplicationContext getApplicationContext() {
        return this.applicationContext;
    }

    private RAIDOneDriver getDriver() {
        return this.driver;
    }

    private void createBuckets() {
        List<ServerBucket> list = getDriver().getVirtualFileSystemService().listAllBuckets();
        startuplogger.info("3. Creating " + String.valueOf(list.size()) + " Buckets");
        for (ServerBucket bucket : list) {
            for (Drive drive : getDriver().getDrivesAll()) {
                if (drive.getDriveInfo().getStatus() == DriveStatus.NOTSYNC) {
                    try {
                        if (!drive.existsBucketById(bucket.getId())) {
                            drive.createBucket(bucket.getBucketMetadata());
                        }
                    } catch (Exception e) {
                        this.errors.getAndIncrement();
                        logger.error(e, SharedConstant.NOT_THROWN);
                        return;
                    }
                }
            }
        }
    }
}
