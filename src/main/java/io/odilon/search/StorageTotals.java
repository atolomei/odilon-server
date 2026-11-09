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
package io.odilon.search;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * Immutable snapshot of the logical storage used, measured from the search
 * index: number of objects and sum of their sizes, per bucket and in total.
 * </p>
 * <p>
 * Sizes are the logical object sizes: they do not include erasure-coding
 * parity, RAID copies, encryption overhead or old versions.
 * </p>
 * <p>
 * {@link #getUnmeasured()} is the number of indexed objects that have no size
 * recorded yet (indexed before the size doc-values field existed). They are
 * included in neither the object nor the byte totals, and become measured when
 * they are re-indexed.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public final class StorageTotals {

	public static final StorageTotals EMPTY = new StorageTotals(Collections.emptyMap(), 0L, null, 0.0);

	/** bucketName -> {objects, bytes} */
	private final Map<String, long[]> perBucket;
	private final long unmeasured;
	private final OffsetDateTime measured;
	private final double scanSeconds;
	private final long totalObjects;
	private final long totalBytes;

	public StorageTotals(Map<String, long[]> perBucket, long unmeasured, OffsetDateTime measured, double scanSeconds) {
		this.perBucket = Collections.unmodifiableMap(new HashMap<String, long[]>(perBucket));
		this.unmeasured = unmeasured;
		this.measured = measured;
		this.scanSeconds = scanSeconds;
		long o = 0, b = 0;
		for (long[] v : perBucket.values()) {
			o += v[0];
			b += v[1];
		}
		this.totalObjects = o;
		this.totalBytes = b;
	}

	public Set<String> buckets() {
		return perBucket.keySet();
	}

	public long objects(String bucketName) {
		long[] v = perBucket.get(bucketName);
		return v == null ? 0L : v[0];
	}

	public long bytes(String bucketName) {
		long[] v = perBucket.get(bucketName);
		return v == null ? 0L : v[1];
	}

	public long totalObjects() {
		return totalObjects;
	}

	public long totalBytes() {
		return totalBytes;
	}

	public long getUnmeasured() {
		return unmeasured;
	}

	/** null if never measured */
	public OffsetDateTime getMeasured() {
		return measured;
	}

	/** seconds the last scan took */
	public double getScanSeconds() {
		return scanSeconds;
	}

	/** seconds since the snapshot was taken; -1 if never measured */
	public double ageSeconds() {
		if (measured == null)
			return -1.0;
		return Duration.between(measured, OffsetDateTime.now()).toMillis() / 1000.0;
	}
}
