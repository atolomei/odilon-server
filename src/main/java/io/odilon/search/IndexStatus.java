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

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.odilon.model.ServerConstant;

/**
 * <p>
 * Status snapshot of the embedded Lucene search index. Durable facts
 * (lastIndexed, lastRebuild) are stored in the Lucene commit userData,
 * volatile ones (pending, errors) are computed at read time.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class IndexStatus implements Serializable {

	private static final long serialVersionUID = 1L;

	public static final String READY = "READY";
	public static final String REBUILDING = "REBUILDING";
	public static final String DEGRADED = "DEGRADED";
	public static final String DISABLED = "DISABLED";

	static DateTimeFormatter fmt = DateTimeFormatter.ofPattern(ServerConstant.DEFAULT_DATE_TIME_FORMAT);
	
	
	@JsonProperty("status")
	public String status = DISABLED;

	@JsonProperty("documents")
	public long documents = 0;

	@JsonProperty("lastIndexed")
	public OffsetDateTime lastIndexed;

	@JsonProperty("lastRebuild")
	public OffsetDateTime lastRebuild;

	@JsonProperty("pending")
	public int pending = 0;

	@JsonProperty("errors")
	public long errors = 0;

	/** progress of a running (or last) reconciliation — meaningful only while rebuildRunning is true */
	@JsonProperty("rebuildRunning")
	public boolean rebuildRunning = false;

	@JsonProperty("rebuildTotalBuckets")
	public int rebuildTotalBuckets = 0;

	@JsonProperty("rebuildProcessedBuckets")
	public int rebuildProcessedBuckets = 0;

	@JsonProperty("rebuildCurrentBucket")
	public String rebuildCurrentBucket;

	@JsonProperty("rebuildProcessedObjects")
	public long rebuildProcessedObjects = 0;

	@JsonProperty("rebuildIndexed")
	public long rebuildIndexed = 0;

	@JsonProperty("rebuildRemoved")
	public long rebuildRemoved = 0;

	@JsonProperty("rebuildStartedAt")
	public long rebuildStartedAt = 0;

	public IndexStatus() {
	}

	/** formatted multi-line block for the startup console */
	public String console() {
		
		StringBuilder str = new StringBuilder();
		str.append("Index Status").append(System.lineSeparator());
		str.append(String.format("Status:        %s%n", status));
		str.append(String.format("Documents:     %,d%n", documents));
		str.append(String.format("Last indexed:  %s%n", lastIndexed != null ? fmt.format(lastIndexed) : "-"));
		str.append(String.format("Last rebuild:  %s%n", lastRebuild != null ? fmt.format(lastRebuild) : "-"));
		str.append(String.format("Pending:       %,d%n", pending));
		str.append(String.format("Errors:        %,d", errors));
		if (rebuildRunning) {
			str.append(System.lineSeparator());
			str.append(String.format("Rebuild:       bucket %d/%d (%s) | objects processed: %,d | indexed: %,d | removed: %,d", rebuildProcessedBuckets, rebuildTotalBuckets,
					rebuildCurrentBucket != null ? rebuildCurrentBucket : "-", rebuildProcessedObjects, rebuildIndexed, rebuildRemoved));
		}
		return str.toString();
	}
}
