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
package io.odilon.virtualFileSystem;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import io.odilon.log.Logger;
import io.odilon.util.Check;
import io.odilon.virtualFileSystem.model.SimpleDrive;

/**
 * <p>
 * Used by: <br/>
 * RAID 0 {@link RAIDZeroDriver} <br/>
 * RAID 1 {@link RAIDOneDriver}. <br/>
 * <br/>
 * This class is not used by RAID 6 {@link RAIDSixDriver}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */


@JsonInclude(Include.NON_NULL)
@NotThreadSafe
@Component
@Scope("prototype")
public class OdilonSimpleDrive extends OdilonDrive implements SimpleDrive {

    static private Logger logger = Logger.getLogger(OdilonSimpleDrive.class.getName());

    /**
     * Constructor called by Spring.io
     * 
     * @param rootDir
     */
    @Autowired
    protected OdilonSimpleDrive(String rootDir) {
        super(rootDir);
    }

    /**
     * <p>
     * Constructor explicit
     * 
     * @param driveNanme
     * @param rootDir
     */
    protected OdilonSimpleDrive(String driveName, String rootDir, int configOrder, String raidSetup, int raidDrives) {
        super(driveName, rootDir, configOrder, raidSetup,  raidDrives);
    }

    /**
     * <p>
     * </p>
     */
    // @Override
    // public String getObjectDataFilePath(Long bucketId, String objectName) {
    // return this.getRootDirPath() + File.separator + bucketId.toString() +
    // File.separator + objectName;
    // }

    // @Override
    // public String getObjectDataVersionFilePath(Long bucketId, String objectName,
    // int version) {
    // return this.getRootDirPath() + File.separator + bucketId.toString() +
    // File.separator + VirtualFileSystemService.VERSION_DIR + File.separator +
    // objectName + VirtualFileSystemService.VERSION_EXTENSION +
    // String.valueOf(version);
    // }

    /**
     * <b>Object Data</b>
     * <p>
     * This method is not ThreadSafe
     * </p>
     * 
     * @Override public InputStream getObjectInputStream(Long bucketId, String
     *           objectName) {
     * 
     *           Check.requireNonNullArgument(bucketId.toString(), "bucketId is
     *           null"); Check.requireNonNullStringArgument(objectName, "objectName
     *           can not be null -> b:" + bucketId.toString());
     * 
     *           try {
     * 
     *           ObjectPath path = new ObjectPath(this, bucketId, objectName); File
     *           file = path.dataFilePath().toFile();
     * 
     *           //return Files.newInputStream(getObjectDataFile(bucketId,
     *           objectName).toPath()); return Files.newInputStream(file.toPath());
     * 
     *           } catch (Exception e) { throw new InternalCriticalException(e, "b:"
     *           + bucketId.toString() + ", o:" + objectName +", d:" + getName()); }
     *           }
     */

    /**
     * <b>bDATA</b>
     * 
     * Dir -> directory Path -> to File File -> File
     * 
     */
    @Override
    public File putObjectStream(Long bucketId, String objectName, InputStream stream) throws IOException {

        Check.requireNonNullArgument(bucketId, "bucketId is null");
        Check.requireNonNullStringArgument(objectName, "objectName can not be null -> b:" + bucketId.toString());

        /** data Bucket created on demand */
        createDataBucketDirIfNotExists(bucketId);

        try {
            ObjectPath path = new ObjectPath(this, bucketId, objectName);
            // String dataFilePath = this.getObjectDataFilePath(bucketId, objectName);
            String dataFilePath = path.dataFilePath().toString();
            transferTo(stream, dataFilePath);
            return new File(dataFilePath);
        } catch (IOException e) {
            logger.error(e.getClass().getName() + " putObjectStream -> " + "b:" + bucketId.toString() + ", o:" + objectName + ", d:"
                    + getName());
            throw (e);
        }
    }

    @Override
    public void putObjectDataFile(Long bucketId, String objectName, File objectFile) throws IOException {
        Check.requireNonNullArgument(bucketId, "bucketId is null");
        try (InputStream is = new FileInputStream(objectFile)) {
            putObjectStream(bucketId, objectName, is);
        }
    }

    @Override
    public void putObjectDataVersionFile(Long bucketId, String objectName, int version, File objectFile) throws IOException {
        Check.requireNonNullArgument(bucketId, "bucketId is null");
        try (InputStream is = new BufferedInputStream(new FileInputStream(objectFile))) {
            putObjectDataVersionStream(bucketId, objectName, version, is);
        }
    }

    // @Override
    // public File getObjectDataFile(Long bucketId, String objectName) {
    // Check.requireNonNullArgument(bucketId, "bucketId is null");
    // return new File(this.getRootDirPath(), bucketId.toString() + File.separator +
    // objectName);
    // }

    // @Override
    // public File getObjectDataVersionFile(Long bucketId, String objectName, int
    // version) {
    // Check.requireNonNullArgument(bucketId, "bucketId is null");
    // return new File(getObjectDataVersionFilePath(bucketId, objectName, version));
    // }

    protected File putObjectDataVersionStream(Long bucketId, String objectName, int version, InputStream stream)
            throws IOException {
        Check.requireNonNullArgument(bucketId, "bucketId is null");
        Check.requireNonNullStringArgument(objectName, "objectName can not be null -> b:" + bucketId.toString());

        try {
            ObjectPath path = new ObjectPath(this, bucketId, objectName);
            String dataFilePath = path.dataFileVersionPath(version).toString();
            // String dataFilePath = this.getObjectDataVersionFilePath(bucketId, objectName,
            // version);

            transferTo(stream, dataFilePath);
            return new File(dataFilePath);
        } catch (IOException e) {
            logger.error(e.getClass().getName() + " -> " + "b:" + bucketId.toString() + ", o:" + objectName + ", d:" + getName());
            throw (e);
        }
    }

}
