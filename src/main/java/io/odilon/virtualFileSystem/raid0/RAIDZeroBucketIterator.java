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
package io.odilon.virtualFileSystem.raid0;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.model.ServerConstant;
import io.odilon.virtualFileSystem.model.BucketIterator;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.ServerBucket;

/**
 * <p>
 * RAID 0. Bucket Iterator <br/>
 * All Drives are enabled in RAID 0 because the Drive sync process is blocking
 * when the {@link VirtualFileSystemService} starts
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class RAIDZeroBucketIterator extends BucketIterator implements Closeable {

    @JsonIgnore
    private Map<Drive, Iterator<Path>> itMap;

    @JsonIgnore
    private Map<Drive, Stream<Path>> streamMap;

    @JsonIgnore
    private List<Drive> drives;

    public RAIDZeroBucketIterator(RAIDZeroDriver driver, ServerBucket bucket, Optional<Long> opOffset,
            Optional<String> opPrefix) {
        this(driver, bucket, opOffset, opPrefix, Optional.empty());
    }

    public RAIDZeroBucketIterator(RAIDZeroDriver driver, ServerBucket bucket, Optional<Long> opOffset,
            Optional<String> opPrefix, Optional<String> serverAgentId) {
        super(driver, bucket);

        opOffset.ifPresent(x -> setOffset(x));
        serverAgentId.ifPresent(x -> setAgentId(x));
        opPrefix.ifPresent(x -> setPrefix(x.toLowerCase().trim()));

        /**
         * after the {@link VirtualFileService} starts up all drives are in state
         * {@link DriveStatus.ENABLED} in RAID 0
         */
        this.drives = new ArrayList<Drive>();
        this.drives.addAll(getDriver().getDrivesEnabled());

        this.streamMap = new HashMap<Drive, Stream<Path>>();
        this.itMap = new HashMap<Drive, Iterator<Path>>();
    }

    @Override
    public synchronized void close() throws IOException {
        if (this.getStreamMap() == null)
            return;
        this.getStreamMap().forEach((k, v) -> v.close());
    }

    @Override
    protected void init() {
        for (Drive drive : getDrives()) {
            Path start = new File(drive.getBucketMetadataDirPath(getBucketId())).toPath();
            Stream<Path> stream = null;
            try {
                stream = Files.walk(start, 1).skip(1).filter(file -> Files.isDirectory(file))
                        .filter(file -> (getPrefix() == null)
                                || (file.getFileName().toString().toLowerCase().trim().startsWith(getPrefix())));
                // filter(file -> isObjectStateEnabled(file));
                this.getStreamMap().put(drive, stream);
            } catch (IOException e) {
                throw new InternalCriticalException(e);
            }
            Iterator<Path> it = stream.iterator();
            this.getItMap().put(drive, it);
        }
        skipOffset();
        setInitiated(true);
    }

    /**
     * 
     */
    private void skipOffset() {

        if (getOffset() == 0)
            return;

        boolean isItems = false;
        {
            for (Drive drive : this.getDrives()) {
                if (this.getItMap().get(drive).hasNext()) {
                    isItems = true;
                    break;
                }
            }
        }
        long skipped = getCumulativeIndex();

        while (isItems && skipped < getOffset()) {
            int d_index = 0;
            int d_poll = d_index++ % this.getDrives().size();
            Drive drive = this.getDrives().get(d_poll);
            Iterator<Path> iterator = getItMap().get(drive);
            if (iterator.hasNext()) {
                iterator.next();
                skipped++;
            } else {
                /** drive has no more items */
                this.getStreamMap().get(drive).close();
                this.getItMap().remove(drive);
                this.getDrives().remove(d_poll);
                isItems = !this.getDrives().isEmpty();
            }
        }
    }

    /**
     * @return
     */
    @Override
    protected boolean fetch() {

        setRelativeIndex(0);
        setBuffer(new ArrayList<Path>());

        boolean isItems = false;

        if (this.getDrives().isEmpty())
            return false;
        {
            for (Drive drive : this.getDrives()) {
                if (this.getItMap().get(drive).hasNext()) {
                    isItems = true;
                    break;
                }
            }
        }
        {
            int buffer_index = 0;
            while (isItems && (getBuffer().size() < ServerConstant.BUCKET_ITERATOR_DEFAULT_BUFFER_SIZE)) {
                int dPoll = buffer_index++ % this.getDrives().size();
                Drive drive = this.getDrives().get(dPoll);
                Iterator<Path> iterator = this.getItMap().get(drive);
                if (iterator.hasNext()) {
                    getBuffer().add(iterator.next());
                } else {
                    /** drive has no more items */
                    this.getStreamMap().get(drive).close();
                    this.getItMap().remove(drive);
                    this.getDrives().remove(dPoll);
                    isItems = !this.getDrives().isEmpty();
                }
            }
        }
        return (!getBuffer().isEmpty());
    }

    protected Map<Drive, Iterator<Path>> getItMap() {
        return itMap;
    }

    protected void setItMap(Map<Drive, Iterator<Path>> itMap) {
        this.itMap = itMap;
    }

    protected Map<Drive, Stream<Path>> getStreamMap() {
        return streamMap;
    }

    protected void setStreamMap(Map<Drive, Stream<Path>> streamMap) {
        this.streamMap = streamMap;
    }

    protected void setDrives(List<Drive> drives) {
        this.drives = drives;
    }

    /**
     * <p>
     * There are no Drives in mode {@link DriveStatus#NOTSYNC} in RAID 0. <br/>
     * All new drives are synced before the VirtualFileSystemService completes its
     * initialization.
     * </p>
     * 
     * @return
     */
    protected List<Drive> getDrives() {
        return this.drives;
    }

}
