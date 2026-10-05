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
package io.odilon.monitor;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>
 * Number of objects uploaded per bucket (and over all buckets, key
 * {@link #ALL}) for a set of time ranges, plus the timestamp the count was
 * made. Built by {@link SystemMonitorService#getObjectsUploaded()} from the
 * search index; the value is cached and may be slightly stale.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ObjectsUploaded implements Serializable {

	private static final long serialVersionUID = 1L;

	/** key of the totals over all buckets */
	public static final String ALL = "All";

	/** totals of one bucket (or of all buckets) */
	public static class Counts implements Serializable {
		private static final long serialVersionUID = 1L;

		public long lastMinute;
		public long lastHour;
		public long today;
		public long yesterday;
		public long last30Days;
		public long last12Months;
		public long allTime;
	}

	/** bucketName -> counts; first entry is {@link #ALL} */
	private final Map<String, Counts> buckets = new LinkedHashMap<String, Counts>();

	/** when the count was made */
	private final OffsetDateTime measured;

	public ObjectsUploaded(OffsetDateTime measured) {
		this.measured = measured;
	}

	public OffsetDateTime getMeasured() {
		return this.measured;
	}

	public Map<String, Counts> getBuckets() {
		return this.buckets;
	}

	public void put(String bucketName, Counts counts) {
		this.buckets.put(bucketName, counts);
	}

	public Counts get(String bucketName) {
		return this.buckets.get(bucketName);
	}
}
