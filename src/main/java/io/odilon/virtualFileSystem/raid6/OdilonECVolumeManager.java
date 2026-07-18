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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;

/**
 * <p>
 * Manages the set of {@link ECVolume}s that compose the server's EC storage
 * layer.
 * </p>
 *
 * <h3>Capacity expansion</h3>
 * <p>
 * A "volume" is an independent EC disk group. When the active volume fills up
 * the administrator:
 * </p>
 * <ol>
 * <li>Mounts a new set of disks.</li>
 * <li>Declares them as a new volume in {@code odilon.properties}.</li>
 * <li>Sets {@code volume.active} to the new volume id.</li>
 * </ol>
 * <p>
 * From that point all new objects go to the new volume; old objects remain
 * readable because every {@link io.odilon.model.ObjectMetadata} stores the
 * {@code volumeId} that was active when the object was created. <strong>No
 * migration is required.</strong>
 * </p>
 *
 * <h3>Thread safety</h3>
 * <p>
 * The volume list is a {@link CopyOnWriteArrayList}; {@code activeVolumeId} is
 * {@code volatile}. Concurrent reads are lock-free; mutations
 * ({@link #addVolume} and {@link #setActiveVolumeId}) are {@code synchronized}.
 * </p>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * @see ECVolume
 * @see VolumeStatus
 */
public class OdilonECVolumeManager {

	static private Logger logger = Logger.getLogger(OdilonECVolumeManager.class.getName());

	/**
	 * Ordered list of all volumes. Index == {@link ECVolume#getVolumeId()}.
	 */
	private final CopyOnWriteArrayList<ECVolume> volumes = new CopyOnWriteArrayList<>();

	/** Which volume receives new-object writes. */
	private volatile int activeVolumeId = 0;

	private volatile List<ECVolume> ecVolume;

	public OdilonECVolumeManager() {
	}

	// ─── Mutation ──────────────────────────────────────────────────────────────

	/**
	 * Register a volume. Volumes <strong>must</strong> be added in volumeId order
	 * (0, 1, 2, …); gaps are not allowed.
	 *
	 * @throws InternalCriticalException if the expected volumeId is not sequential.
	 */
	public synchronized void addVolume(ECVolume volume) {
		int expected = volumes.size();
		if (volume.getVolumeId() != expected)
			throw new InternalCriticalException("Volumes must be added in order. Expected volumeId=" + expected + " but received volumeId=" + volume.getVolumeId());
		volumes.add(volume);
		logger.info("Registered -> " + volume);
	}

	/**
	 * Change the active write volume.
	 *
	 * @throws InternalCriticalException if the target volume is not
	 *                                   {@link VolumeStatus#ACTIVE} (i.e., not
	 *                                   writable).
	 */
	public synchronized void setActiveVolumeId(int volumeId) {
		ECVolume target = getVolumeById(volumeId);
		if (!target.isActive())
			throw new InternalCriticalException("Cannot set volume " + volumeId + " as active: status is " + target.getStatus());
		this.activeVolumeId = volumeId;
		logger.info("Active volume changed to volumeId=" + volumeId);
	}

	// ─── Query ─────────────────────────────────────────────────────────────────

	/**
	 * Returns the volume that currently accepts new-object writes.
	 */
	public ECVolume getActiveVolume() {
		return getVolumeById(activeVolumeId);
	}

	/**
	 * Look up a volume by its identifier.
	 *
	 * @throws InternalCriticalException if no volume with that id exists.
	 */
	public ECVolume getVolumeById(int volumeId) {
		if (volumeId < 0 || volumeId >= volumes.size())
			throw new InternalCriticalException("No volume found for volumeId=" + volumeId + "  (total registered volumes=" + volumes.size() + ')');
		return volumes.get(volumeId);
	}

	/** Returns an unmodifiable snapshot of all registered volumes. */
	public List<ECVolume> getAllVolumes() {
		return Collections.unmodifiableList(new ArrayList<>(volumes));
	}

	/**
	 * <p>
	 * Returns volumes in the order they should be searched for object metadata when
	 * the owning volume is not yet known (cache-miss path).
	 * </p>
	 * <ul>
	 * <li>Active volume first — most recently-written objects live here.</li>
	 * <li>Then all other volumes, sorted by {@code volumeId} <em>descending</em>
	 * (newest archived volume first, oldest last) — reduces scan time for objects
	 * written just before a volume transition.</li>
	 * </ul>
	 */

	public List<ECVolume> getVolumesInSearchOrder() {

		if (ecVolume != null)
			return this.ecVolume;

		synchronized (this) {
			// Inner null-check: required for correct DCL — another thread may have
			// computed the list between the outer check and acquiring the monitor.
			if (ecVolume != null)
				return this.ecVolume;

			List<ECVolume> order = new ArrayList<>();
			ECVolume active = getActiveVolume();
			order.add(active);
			new ArrayList<>(volumes).stream().filter(v -> v.getVolumeId() != activeVolumeId).sorted(Comparator.comparingInt(ECVolume::getVolumeId).reversed()).forEach(order::add);
			this.ecVolume = Collections.unmodifiableList(order);
		}
		return this.ecVolume;
	}

	/** Currently active volume id. */
	public int getActiveVolumeId() {
		return activeVolumeId;
	}

	/** Total number of registered volumes. */
	public int size() {
		return volumes.size();
	}

	/** {@code true} when more than one volume has been registered. */
	public boolean hasMultipleVolumes() {
		return volumes.size() > 1;
	}
}
