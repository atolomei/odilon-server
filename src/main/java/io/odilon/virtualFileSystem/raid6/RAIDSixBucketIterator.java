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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.concurrent.ThreadSafe;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.errors.InternalCriticalException;
import io.odilon.model.ObjectMetadata;
import io.odilon.virtualFileSystem.model.BucketIterator;
import io.odilon.virtualFileSystem.model.Drive;
import io.odilon.virtualFileSystem.model.DriveStatus;
import io.odilon.virtualFileSystem.model.ServerBucket;

/**
 * <p>
 * <b>RAID 6 Bucket iterator</b> <br/>
 * Data is partitioned into blocks encoded using RS Erasure code and stored on
 * all drives.<br/>
 * The encoding convention for blocks in File System is detailed in
 * {@link RAIDSixDriver}
 * </p>
 *
 * <p>
 * This {@link BucketIterator} uses the first enabled {@link Drive} of
 * <em>each</em> volume to iterate {@link ObjectMetadata} instances, then merges
 * the per-volume streams.  This is necessary because in a multi-volume
 * deployment objects on different volumes have their metadata stored on
 * different sets of drives: randomly picking a single drive from the flat
 * {@code drivesEnabled} list would silently miss all objects on all other
 * volumes.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@ThreadSafe
public class RAIDSixBucketIterator extends BucketIterator implements Closeable {

	/**
	 * One representative (first enabled) drive per volume.
	 * Objects on volume N are listed via drives.get(N).
	 */
	@JsonProperty("drives")
	private final List<Drive> drives;

	@JsonIgnore
	private Iterator<Path> iterator;

	@JsonIgnore
	private Stream<Path> stream;

	public RAIDSixBucketIterator(RAIDSixDriver driver, ServerBucket bucket, Optional<Long> opOffset, Optional<String> opPrefix) {
		super(driver, bucket);
		opPrefix.ifPresent(x -> setPrefix(x.toLowerCase().trim()));
		opOffset.ifPresent(x -> setOffset(x));

		// Pick the first ENABLED drive from each volume so we list exactly the objects
		// stored on that volume. Using all drives from all volumes via the flat
		// drivesEnabled list would cause every object to be returned once per drive
		// within its volume; using a single random drive misses all other volumes.
		List<Drive> repDrives = new ArrayList<>();
		for (RAIDSixVolume vol : driver.getVolumeManager().getAllVolumes()) {

			// Bug 2 fix: ARCHIVED means "no I/O accepted" — skip entirely.
			if (!vol.isActive() && !vol.isReadable())
				continue;

			// Bug 1 fix: apply the NOTSYNC fallback per-volume, not globally.
			// A volume whose drives are all NOTSYNC (e.g. mid-sync on first boot) must
			// still contribute its first drive; without the per-volume check it would be
			// silently skipped and all its objects would be invisible.
			Optional<Drive> rep = vol.getDrives().stream()
					.filter(d -> d.getDriveInfo() != null
							&& d.getDriveInfo().getStatus() == DriveStatus.ENABLED)
					.findFirst();

			if (rep.isPresent()) {
				repDrives.add(rep.get());
			} else if (!vol.getDrives().isEmpty()) {
				// Fallback: volume has drives but none is ENABLED yet — use the first one.
				repDrives.add(vol.getDrives().get(0));
			}
		}
		this.drives = repDrives;
	}

	@Override
	public synchronized void close() throws IOException {
		if (getStream() != null)
			getStream().close();
	}

	/**
	 * Initializes a merged stream of metadata-directory paths from every volume's
	 * representative drive. Each volume contributes only the objects stored on it,
	 * so the union covers the full bucket namespace across all volumes.
	 *
	 * <p>Bug 3 fix: streams are collected into an explicit list before being merged.
	 * If {@link Files#walk} throws on drive N, all streams opened for drives 0..N-1
	 * are closed before re-throwing, preventing resource leaks that the old
	 * {@code Stream.concat} chain left behind.</p>
	 */
	@Override
	protected void init() {
		List<Stream<Path>> streams = new ArrayList<>();
		try {
			for (Drive d : this.drives) {
				Path start = new File(d.getBucketMetadataDirPath(getBucket())).toPath();
				if (!start.toFile().exists())
					continue;
				Stream<Path> volStream = Files.walk(start, 1)
						.skip(1)
						.filter(Files::isDirectory)
						.filter(p -> (getPrefix() == null) || p.getFileName().toString().toLowerCase().startsWith(getPrefix()))
						.filter(p -> isValidState(p));
				streams.add(volStream);
			}
			this.stream = streams.stream().reduce(Stream.empty(), Stream::concat);
		} catch (IOException e) {
			// Close every stream that was successfully opened before the failure.
			streams.forEach(Stream::close);
			throw new InternalCriticalException(e);
		}
		this.iterator = this.stream.iterator();
		skipOffset();
		setInitiated(true);
	}

	/**
	 * <p>
	 * No need to synchronize because it is called from the synchronized method
	 * {@link BucketIterator#hasNext}
	 * </p>
	 * 
	 * @return false if there are no more items
	 */
	@Override
	protected boolean fetch() {
		setRelativeIndex(0);
		setBuffer(new ArrayList<Path>());
		boolean isItems = true;
		while (isItems && getBuffer().size() < defaultBufferSize()) {
			if (getIterator().hasNext())
				getBuffer().add(getIterator().next());
			else
				isItems = false;
		}
		return !getBuffer().isEmpty();
	}

	private void skipOffset() {
		if (getOffset() == 0)
			return;
		boolean isItems = true;
		int skipped = 0;
		while (isItems && skipped < getOffset()) {
			if (this.getIterator().hasNext()) {
				this.getIterator().next();
				skipped++;
			} else {
				break;
			}
		}
	}

	private Stream<Path> getStream() {
		return this.stream;
	}

	private Iterator<Path> getIterator() {
		return this.iterator;
	}
}
