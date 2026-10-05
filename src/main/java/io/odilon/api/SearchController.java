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
package io.odilon.api;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.odilon.error.OdilonServerAPIException;
import io.odilon.log.Logger;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.model.SharedConstant;
import io.odilon.scheduler.SchedulerService;
import io.odilon.search.IndexStatus;
import io.odilon.search.SearchIndexReconciler;
import io.odilon.search.SearchQuery;
import io.odilon.search.SearchResult;
import io.odilon.search.SearchService;
import io.odilon.service.ObjectStorageService;
import io.odilon.traffic.TrafficControlService;
import io.odilon.traffic.TrafficPass;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Search API over the embedded Lucene index (bucket / object names, metadata
 * and tags). Lucene is a secondary rebuildable index — results contain only
 * locating and display information, the objects live in Odilon storage.
 * </p>
 * <ul>
 * <li>{@code GET /search?bucket=&q=&meta.<key>=<value>&offset=&maxResults=}</li>
 * <li>{@code GET /search/status}</li>
 * </ul>
 * 
 * curl -X POST http://localhost:9600/search/rebuild -u odilon:odilon
 
 
 
 
 # Index status (documents, pending queue, lastIndexed, lastRebuild, state)
curl -u odilon:odilon "http://localhost:9600/search/status"

# Trigger a full index rebuild (returns 202; 409 if already running)
curl -X POST -u odilon:odilon "http://localhost:9600/search/rebuild"

# Full-text search on the _all field (object key, filename, tags, metadata values)
curl -u odilon:odilon "http://localhost:9600/search?q=invoice"

# Search by custom metadata key "title" (exact match on meta.title)
curl -u odilon:odilon "http://localhost:9600/search?meta.title=Annual%20Report%202024"

# Full-text on title content (analyzed field, if "title" is whitelisted)
curl -u odilon:odilon "http://localhost:9600/search?q=annual+report"

# Restrict to a bucket
curl -u odilon:odilon "http://localhost:9600/search?bucket=documents&q=contract"

# Combine bucket + metadata filters
curl -u odilon:odilon "http://localhost:9600/search?bucket=photos&meta.author=alejandro&meta.year=2024"

# Metadata filter + free text together
curl -u odilon:odilon "http://localhost:9600/search?q=budget&meta.department=finance"

# Pagination
curl -u odilon:odilon "http://localhost:9600/search?q=pdf&offset=100&maxResults=50"

# Max results cap test (server caps at 1000)
curl -u odilon:odilon "http://localhost:9600/search?q=*&maxResults=5000"


 
 
 # Matches any object whose file name contains the token "tolomei" (any case)
curl -u odilon:odilon "http://localhost:9600/search?q=tolomei"
 
 # "tolomei" in the file name only (any case)
curl -u odilon:odilon "http://localhost:9600/search?fileName=tolomei"

# "tolomei" in the object key only
curl -u odilon:odilon "http://localhost:9600/search?objectName=tolomei"

# objects modified in September 2026
curl -u odilon:odilon "http://localhost:9600/search?lastModifiedFrom=2026-09-01&lastModifiedTo=2026-09-30"

# precise timestamps
curl -u odilon:odilon "http://localhost:9600/search?lastModifiedFrom=2026-09-30T12:00:00Z"

# combined: filename + date range + bucket + metadata
curl -u odilon:odilon "http://localhost:9600/search?bucket=docs&fileName=tolomei&lastModifiedFrom=2026-01-01&meta.department=legal"

 
 
 -------
 
 curl examples (remember %2A = *, %22 = ")
 
 # token match — tolomei-report.pdf, Tolomei_2024.docx
curl -u odilon:odilon "http://localhost:9600/search?fileName=tolomei"

# partial match anywhere, any case — atolomei.pdf, XTOLOMEIx.doc
curl -u odilon:odilon "http://localhost:9600/search?fileName=%2Atolomei%2A"

# prefix match (faster — no leading wildcard)
curl -u odilon:odilon "http://localhost:9600/search?fileName=tolomei%2A"

# exact name, case-insensitive — matches Tolomei-Report.pdf, TOLOMEI-REPORT.PDF
curl -u odilon:odilon "http://localhost:9600/search?fileName=%22tolomei-report.pdf%22"

# exact object key
curl -u odilon:odilon "http://localhost:9600/search?objectName=%22reports/2026/tolomei.pdf%22"


# single-char wildcard
In a URL, a literal ? marks the start of the query string, so when ? is part of a parameter value it must be percent-encoded as %3F. In the example:
the server receives fileName=tolomei-v?.pdf, where ? is the Lucene single-character wildcard — matching e.g. tolomei-v1.pdf, tolomei-vX.pdf.


curl -u odilon:odilon "http://localhost:9600/search?fileName=tolomei-v%3F.pdf"




 
 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@RestController
public class SearchController extends BaseApiController {

	static private Logger logger = Logger.getLogger(SearchController.class.getName());

	static final int DEFAULT_MAX_RESULTS = 100;

	private final SearchService searchService;
	private final SchedulerService schedulerService;
	private final SearchIndexReconciler searchIndexReconciler;

	@Autowired
	public SearchController(ObjectStorageService objectStorageService, VirtualFileSystemService virtualFileSystemService, SystemMonitorService monitoringService, TrafficControlService trafficControlService,
			SearchService searchService, SchedulerService schedulerService, SearchIndexReconciler searchIndexReconciler) {
		super(objectStorageService, virtualFileSystemService, monitoringService, trafficControlService);
		this.searchService = searchService;
		this.schedulerService = schedulerService;
		this.searchIndexReconciler = searchIndexReconciler;
	}

	/**
	 * <ul>
	 * <li>{@code q} — free text over the catch-all field</li>
	 * <li>{@code objectName} — case-insensitive tokenized match on the object key only</li>
	 * <li>{@code fileName} — case-insensitive tokenized match on the file name only</li>
	 * <li>{@code lastModifiedFrom} / {@code lastModifiedTo} — ISO-8601 (e.g.
	 * {@code 2026-01-01T00:00:00Z} or {@code 2026-01-01} for date-only), inclusive</li>
	 * <li>{@code meta.<key>=<value>} — exact-match metadata filters</li>
	 * </ul>
	 */
	@RequestMapping(value = "/search", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<List<SearchResult>> search(@RequestParam(required = false) String bucket, @RequestParam(required = false) String q,
			@RequestParam(required = false) String objectName, @RequestParam(required = false) String fileName,
			@RequestParam(required = false) String lastModifiedFrom, @RequestParam(required = false) String lastModifiedTo,
			@RequestParam(required = false, defaultValue = "0") int offset, @RequestParam(required = false, defaultValue = "" + DEFAULT_MAX_RESULTS) int maxResults,
			@RequestParam Map<String, String> allParams) {

		TrafficPass pass = null;

		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (!getSearchService().isEnabled())
				throw new OdilonServerAPIException("search is not enabled (search.enabled=false)");

			SearchQuery sq = new SearchQuery();
			sq.bucketName = bucket;
			sq.q = q;
			sq.objectName = objectName;
			sq.fileName = fileName;
			sq.lastModifiedFrom = parseDate(lastModifiedFrom, false);
			sq.lastModifiedTo = parseDate(lastModifiedTo, true);
			sq.offset = Math.max(0, offset);
			sq.maxResults = Math.min(Math.max(1, maxResults), 1000);

			/** meta.<key>=<value> params -> exact-match metadata filters */
			for (Map.Entry<String, String> entry : allParams.entrySet())
				if (entry.getKey().startsWith("meta."))
					sq.metadata.put(entry.getKey().substring("meta.".length()), entry.getValue());

			List<SearchResult> results = getSearchService().search(sq);
			return new ResponseEntity<List<SearchResult>>(results, HttpStatus.OK);

		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * Accepts ISO-8601 date-time with offset ({@code 2026-01-01T00:00:00Z}) or
	 * date-only ({@code 2026-01-01}). For date-only values, {@code endOfDay}
	 * selects 00:00:00 (from) or 23:59:59.999 (to) so ranges are inclusive.
	 */
	private OffsetDateTime parseDate(String value, boolean endOfDay) {
		if (value == null || value.isBlank())
			return null;
		try {
			return OffsetDateTime.parse(value);
		} catch (DateTimeParseException e) {
			try {
				LocalDate date = LocalDate.parse(value);
				return endOfDay ? date.atTime(LocalTime.MAX).atOffset(ZoneOffset.UTC) : date.atStartOfDay().atOffset(ZoneOffset.UTC);
			} catch (DateTimeParseException e2) {
				throw new OdilonServerAPIException("invalid date format (expected ISO-8601, e.g. 2026-01-01 or 2026-01-01T00:00:00Z) -> " + value);
			}
		}
	}

	@RequestMapping(value = "/search/status", produces = "application/json", method = RequestMethod.GET)
	public ResponseEntity<IndexStatus> status() {

		TrafficPass pass = null;

		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			IndexStatus status = getSearchService().getIndexStatus();
			status.pending = getSchedulerService().getSearchQueueSize();
			return new ResponseEntity<IndexStatus>(status, HttpStatus.OK);

		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	/**
	 * <p>
	 * Admin trigger for a full index rebuild (re-indexes every object regardless
	 * of etag and removes orphans). Runs on a background thread — the endpoint
	 * returns immediately; progress can be followed via {@code /search/status}
	 * (status REBUILDING).
	 * </p>
	 */
	@RequestMapping(value = "/search/rebuild", produces = "application/json", method = RequestMethod.POST)
	public ResponseEntity<String> rebuild() {

		TrafficPass pass = null;

		try {
			pass = getTrafficControlService().getPass(this.getClass().getSimpleName());

			if (!getSearchService().isEnabled())
				throw new OdilonServerAPIException("search is not enabled (search.enabled=false)");

			if (getSearchIndexReconciler().isRunning())
				return new ResponseEntity<String>("{\"result\":\"already running\"}", HttpStatus.CONFLICT);

			Thread thread = new Thread(() -> {
				try {
					getSearchIndexReconciler().reconcile(true);
				} catch (Exception e) {
					logger.error(e, SharedConstant.NOT_THROWN);
				}
			});
			thread.setDaemon(true);
			thread.setName("search-index-rebuild");
			thread.start();

			return new ResponseEntity<String>("{\"result\":\"rebuild started\"}", HttpStatus.ACCEPTED);

		} finally {
			getTrafficControlService().release(pass);
			mark();
		}
	}

	public SearchIndexReconciler getSearchIndexReconciler() {
		return this.searchIndexReconciler;
	}

	public SearchService getSearchService() {
		return this.searchService;
	}

	public SchedulerService getSchedulerService() {
		return this.schedulerService;
	}
}
