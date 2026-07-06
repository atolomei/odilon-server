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
package io.odilon.virtualFileSystem.raid6;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.virtualFileSystem.model.Drive;

/**
 * <p>
 * A {@code ECVolume} is an independent Erasure-Coding storage
 * unit composed of a <em>fixed-size</em> set of drives.
 * </p>
 *
 * <h3>Capacity expansion without data migration</h3>
 * <p>
 * When the disks in the current volume are full the administrator provisions a
 * new set of drives, declares a second volume in {@code odilon.properties} and
 * sets {@code volume.active} to point to it. From that moment all new objects
 * are written to the new volume while the old volume remains readable.
 * Every {@link io.odilon.model.ObjectMetadata} stores a {@code volumeId} so
 * the server always knows exactly which volume holds the object's shards.
 * </p>
 *
 * <h3>Shard naming</h3>
 * <p>
 * Shard files use a <em>volume-local</em> disk index (0 …
 * {@code dataDrives + parityDrives - 1}) that corresponds to the position of
 * the drive inside this volume's {@link #getDrives()} list – <em>not</em> the
 * server-global {@link Drive#getConfigOrder()}.
 * </p>
 * <pre>
 *   objectName.&lt;chunk&gt;.&lt;volumeLocalDiskIndex&gt;
 *   objectName.&lt;chunk&gt;.&lt;volumeLocalDiskIndex&gt;.v&lt;version&gt;
 * </pre>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * @see OdilonECVolumeManager
 * @see VolumeStatus
 */
public class ECVolume {

    /** 0-based identifier, persisted in {@link io.odilon.model.ObjectMetadata#volumeId}. */
    @JsonProperty("volumeId")
    private final int volumeId;

    @JsonProperty("status")
    private volatile VolumeStatus status;

    /** Number of data shards (drives that store payload fragments). */
    @JsonProperty("dataDrives")
    private final int dataDrives;

    /** Number of parity shards (drives that store redundancy fragments). */
    @JsonProperty("parityDrives")
    private final int parityDrives;

    /** ISO-8601 timestamp of volume creation. */
    @JsonProperty("created")
    private final OffsetDateTime created;

    /**
     * Ordered list of drives in this volume.
     * <strong>Position {@code i} = volume-local disk index</strong> used in shard
     * file names (see class Javadoc).
     */
    @JsonIgnore
    private final List<Drive> drives;

    /**
     * Map: volume-local disk index → Drive.
     * Built from {@link #drives} at construction time; used by
     * {@link ECDecoder} to locate shards without iterating the full list.
     */
    @JsonIgnore
    private final Map<Integer, Drive> drivesRSDecode;

    // ─── Constructors ──────────────────────────────────────────────────────────

    /**
     * Convenience constructor – status defaults to {@link VolumeStatus#ACTIVE} and
     * {@code created} is set to "now".
     */
    public ECVolume(int volumeId, int dataDrives, int parityDrives, List<Drive> orderedDrives) {
        this(volumeId, dataDrives, parityDrives, orderedDrives, VolumeStatus.ACTIVE, OffsetDateTime.now());
    }

    public ECVolume(int volumeId, int dataDrives, int parityDrives,
                         List<Drive> orderedDrives, VolumeStatus status, OffsetDateTime created) {
        this.volumeId     = volumeId;
        this.dataDrives   = dataDrives;
        this.parityDrives = parityDrives;
        this.status       = status;
        this.created      = created;

        List<Drive> copy = new ArrayList<>(orderedDrives);
        this.drives       = Collections.unmodifiableList(copy);

        Map<Integer, Drive> map = new HashMap<>();
        for (int i = 0; i < copy.size(); i++)
            map.put(i, copy.get(i));
        this.drivesRSDecode = Collections.unmodifiableMap(map);
    }

    // ─── Accessors ─────────────────────────────────────────────────────────────

    /** 0-based volume identifier, stored in every object's metadata. */
    public int getVolumeId()      { return volumeId;  }

    /** Number of data drives (Reed–Solomon data shards). */
    public int getDataDrives()    { return dataDrives;   }

    /** Number of parity drives (Reed–Solomon parity shards). */
    public int getParityDrives()  { return parityDrives; }

    /** Total shards = {@code dataDrives + parityDrives}. */
    public int getTotalShards()   { return dataDrives + parityDrives; }

    /** Timestamp when this volume was registered. */
    public OffsetDateTime getCreated() { return created; }

    /** Current lifecycle status. */
    public VolumeStatus getStatus() { return status; }

    /** Change the status (thread-safe via {@code volatile}). */
    public void setStatus(VolumeStatus status) { this.status = status; }

    /** Returns {@code true} when the volume accepts writes. */
    public boolean isActive()    { return status == VolumeStatus.ACTIVE;    }

    /** Returns {@code true} when the volume accepts reads. */
    public boolean isReadable()  { return status == VolumeStatus.ACTIVE || status == VolumeStatus.READONLY; }

    /**
     * Ordered drive list; position {@code i} is the volume-local disk index used
     * in shard file names.  <strong>Do not mutate.</strong>
     */
    public List<Drive> getDrives() { return drives; }

    /**
     * Map from volume-local disk index (0 …
     * {@link #getTotalShards()}{@code -1}) to {@link Drive}.
     * Used by the Reed–Solomon decoder to locate shard files.
     */
    public Map<Integer, Drive> getDrivesRSDecode() { return drivesRSDecode; }

    // ─── Object overrides ──────────────────────────────────────────────────────

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+"{id=" + volumeId
                + ", status=" + status
                + ", data=" + dataDrives
                + ", parity=" + parityDrives
                + ", drives=" + drives.size()
                + ", created=" + created + '}';
    }
}
