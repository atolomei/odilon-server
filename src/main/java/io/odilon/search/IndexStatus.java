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

	public IndexStatus() {
	}

	/** formatted multi-line block for the startup console */
	public String console() {
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		StringBuilder str = new StringBuilder();
		str.append("Index Status").append(System.lineSeparator());
		str.append(String.format("Status:        %s%n", status));
		str.append(String.format("Documents:     %,d%n", documents));
		str.append(String.format("Last indexed:  %s%n", lastIndexed != null ? fmt.format(lastIndexed) : "-"));
		str.append(String.format("Last rebuild:  %s%n", lastRebuild != null ? fmt.format(lastRebuild) : "-"));
		str.append(String.format("Pending:       %,d%n", pending));
		str.append(String.format("Errors:        %,d", errors));
		return str.toString();
	}
}
