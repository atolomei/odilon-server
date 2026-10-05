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

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.FSDirectory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServiceStatus;
import io.odilon.model.SharedConstant;
import io.odilon.service.BaseService;
import io.odilon.service.ServerSettings;
import io.odilon.util.Check;

/**
 * <p>
 * Apache Lucene implementation of {@link SearchService}.
 * </p>
 * <p>
 * Owns a single {@link IndexWriter} and a near-real-time
 * {@link SearcherManager} over the directory configured by
 * {@code search.index.dir}. Document identity is the term
 * {@code _id = bucketName:objectName}; {@code updateDocument} makes every
 * upsert idempotent.
 * </p>
 * <h3>Document schema</h3>
 * <ul>
 * <li>{@code bucket} — StringField (exact) + stored</li>
 * <li>{@code objectKey} — StringField (exact) + stored; {@code objectKey_txt}
 * tokenized</li>
 * <li>{@code fileName} — stored; {@code fileName_txt} tokenized</li>
 * <li>{@code size} — LongPoint + stored</li>
 * <li>{@code contentType} — StringField + stored</li>
 * <li>{@code etag} — StringField + stored</li>
 * <li>{@code lastModified} — LongPoint + stored (epoch millis)</li>
 * <li>{@code tags} — multi-valued StringField + stored; {@code tags_txt}
 * tokenized</li>
 * <li>{@code meta.<key>} — exact StringField; {@code meta_txt.<key>}
 * tokenized. Custom metadata keys taken from {@code customTags} entries of the
 * form {@code key:value}, optionally whitelisted by
 * {@code search.metadata.keys}</li>
 * <li>{@code _all} — catch-all TextField for default queries</li>
 * </ul>
 * <p>
 * Durable status facts ({@code lastIndexed}, {@code lastRebuild},
 * {@code schemaVersion}, {@code errorCount}) are kept in the Lucene commit
 * userData — atomic with the index state itself.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class LuceneSearchService extends BaseService implements SearchService {

	static private Logger logger = Logger.getLogger(LuceneSearchService.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	public static final String SCHEMA_VERSION = "2";

	/** field names */
	static final String F_ID = "_id";
	static final String F_BUCKET = "bucket";
	static final String F_OBJECT_KEY = "objectKey";
	static final String F_OBJECT_KEY_TXT = "objectKey_txt";
	/** lowercased, un-analyzed copy — case-insensitive exact / wildcard match */
	static final String F_OBJECT_KEY_LC = "objectKey_lc";
	static final String F_FILENAME = "fileName";
	static final String F_FILENAME_TXT = "fileName_txt";
	/** lowercased, un-analyzed copy — case-insensitive exact / wildcard match */
	static final String F_FILENAME_LC = "fileName_lc";
	static final String F_SIZE = "size";
	static final String F_CONTENT_TYPE = "contentType";
	static final String F_ETAG = "etag";
	static final String F_LAST_MODIFIED = "lastModified";
	static final String F_TAGS = "tags";
	static final String F_TAGS_TXT = "tags_txt";
	static final String F_META_PREFIX = "meta.";
	static final String F_META_TXT_PREFIX = "meta_txt.";
	static final String F_ALL = "_all";

	/** commit userData keys */
	static final String UD_LAST_INDEXED = "lastIndexed";
	static final String UD_LAST_REBUILD = "lastRebuild";
	static final String UD_SCHEMA_VERSION = "schemaVersion";
	static final String UD_ERROR_COUNT = "errorCount";

	/** cap on dynamic metadata fields per document */
	static final int MAX_META_FIELDS = 64;

	@JsonIgnore
	private final ServerSettings serverSettings;

	@JsonIgnore
	private FSDirectory directory;

	@JsonIgnore
	private IndexWriter writer;

	@JsonIgnore
	private SearcherManager searcherManager;

	@JsonIgnore
	private Analyzer analyzer;

	@JsonIgnore
	private final AtomicLong errorCount = new AtomicLong(0);

	@JsonIgnore
	private volatile boolean rebuilding = false;

	@JsonIgnore
	private List<String> metadataKeyWhitelist = new ArrayList<String>();

	public LuceneSearchService(ServerSettings serverSettings) {
		this.serverSettings = serverSettings;
	}

	@PostConstruct
	protected void onInitialize() {
		synchronized (this) {
			setStatus(ServiceStatus.STARTING);

			if (!this.serverSettings.isSearchEnabled()) {
				setStatus(ServiceStatus.STOPPED);
				startuplogger.debug("Search -> disabled");
				return;
			}

			try {
				File dir = new File(this.serverSettings.getSearchIndexDir());
				if (!dir.exists())
					dir.mkdirs();

				this.metadataKeyWhitelist = this.serverSettings.getSearchMetadataKeys();
				this.analyzer = new StandardAnalyzer();
				this.directory = FSDirectory.open(dir.toPath());

				IndexWriterConfig config = new IndexWriterConfig(this.analyzer);
				config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
				this.writer = new IndexWriter(this.directory, config);

				/** detect schema change -> flagged DEGRADED until rebuild */
				Map<String, String> userData = getCommitUserData();
				String schema = userData.get(UD_SCHEMA_VERSION);
				if (schema != null && !SCHEMA_VERSION.equals(schema))
					logger.error("Lucene index schema version mismatch (index: " + schema + ", server: " + SCHEMA_VERSION + "). A full rebuild is required." + SharedConstant.NOT_THROWN);

				String errors = userData.get(UD_ERROR_COUNT);
				if (errors != null) {
					try {
						this.errorCount.set(Long.parseLong(errors));
					} catch (NumberFormatException e) {
					}
				}

				/** ensure there is at least one commit so a reader can open */
				if (!DirectoryReader.indexExists(this.directory))
					commit(null);

				this.searcherManager = new SearcherManager(this.writer, null);

				setStatus(ServiceStatus.RUNNING);
				startuplogger.debug("Started -> " + SearchService.class.getSimpleName() + " | dir: " + dir.getAbsolutePath());

			} catch (Exception e) {
				setStatus(ServiceStatus.STOPPED);
				logger.error(e, "Lucene index could not be opened. Search will be unavailable" + SharedConstant.NOT_THROWN);
			}
		}
	}

	@PreDestroy
	protected void onDestroy() {
		try {
			if (this.searcherManager != null)
				this.searcherManager.close();
			if (this.writer != null)
				this.writer.close();
			if (this.directory != null)
				this.directory.close();
		} catch (IOException e) {
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	@Override
	public boolean isEnabled() {
		return this.serverSettings.isSearchEnabled() && getStatus() == ServiceStatus.RUNNING;
	}

	@Override
	public void index(ObjectMetadata meta) {

		Check.requireNonNullArgument(meta, "meta is null");

		if (!isEnabled())
			return;

		try {
			Document doc = toDocument(meta);
			this.writer.updateDocument(new Term(F_ID, id(meta.bucketName, meta.objectName)), doc);
			commit(OffsetDateTime.now());
			this.searcherManager.maybeRefresh();
		} catch (IOException e) {
			incrementErrorCount();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(String bucketName, String objectName) {

		if (!isEnabled())
			return;

		try {
			this.writer.deleteDocuments(new Term(F_ID, id(bucketName, objectName)));
			commit(OffsetDateTime.now());
			this.searcherManager.maybeRefresh();
		} catch (IOException e) {
			incrementErrorCount();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void deleteBucket(String bucketName) {

		if (!isEnabled())
			return;

		try {
			this.writer.deleteDocuments(new Term(F_BUCKET, bucketName));
			commit(OffsetDateTime.now());
			this.searcherManager.maybeRefresh();
		} catch (IOException e) {
			incrementErrorCount();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void renameBucket(String oldBucketName, String newBucketName) {

		if (!isEnabled())
			return;

		/**
		 * Lucene has no in-place update: read all docs of the bucket, re-add them
		 * under the new bucket name, then delete the old ones — all before a single
		 * commit, so the operation is atomic at the index level.
		 */
		try {
			this.searcherManager.maybeRefreshBlocking();
			IndexSearcher searcher = this.searcherManager.acquire();
			try {
				Query q = new TermQuery(new Term(F_BUCKET, oldBucketName));
				TopDocs top = searcher.search(q, Integer.MAX_VALUE);
				StoredFields storedFields = searcher.getIndexReader().storedFields();
				for (ScoreDoc sd : top.scoreDocs) {
					Document old = storedFields.document(sd.doc);
					Document renamed = rekey(old, newBucketName);
					this.writer.updateDocument(new Term(F_ID, renamed.get(F_ID)), renamed);
				}
				this.writer.deleteDocuments(new Term(F_BUCKET, oldBucketName));
				commit(OffsetDateTime.now());
				this.searcherManager.maybeRefresh();
			} finally {
				this.searcherManager.release(searcher);
			}
		} catch (IOException e) {
			incrementErrorCount();
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<SearchResult> search(String bucketName, String query, Map<String, String> metadata, int offset, int maxResults) {
		SearchQuery sq = new SearchQuery();
		sq.bucketName = bucketName;
		sq.q = query;
		sq.metadata = metadata;
		sq.offset = offset;
		sq.maxResults = maxResults;
		return search(sq);
	}

	@Override
	public List<SearchResult> search(SearchQuery sq) {

		List<SearchResult> results = new ArrayList<SearchResult>();

		if (!isEnabled())
			return results;

		try {
			this.searcherManager.maybeRefreshBlocking();
			IndexSearcher searcher = this.searcherManager.acquire();
			try {
				Query finalQuery = buildQuery(sq);

				int offset = Math.max(0, sq.offset);
				int maxResults = Math.max(1, sq.maxResults);
				int limit = offset + maxResults;

				TopDocs top = searcher.search(finalQuery, limit);
				StoredFields storedFields = searcher.getIndexReader().storedFields();

				for (int i = offset; i < top.scoreDocs.length; i++) {
					ScoreDoc sd = top.scoreDocs[i];
					results.add(toResult(storedFields.document(sd.doc), sd.score));
				}
				return results;

			} finally {
				this.searcherManager.release(searcher);
			}
		} catch (Exception e) {
			incrementErrorCount();
			throw new RuntimeException(e);
		}
	}

	@Override
	public long count(SearchQuery sq) {

		if (!isEnabled())
			return 0;

		try {
			this.searcherManager.maybeRefreshBlocking();
			IndexSearcher searcher = this.searcherManager.acquire();
			try {
				return searcher.count(buildQuery(sq));
			} finally {
				this.searcherManager.release(searcher);
			}
		} catch (Exception e) {
			incrementErrorCount();
			throw new RuntimeException(e);
		}
	}

	/** builds the Lucene query from the structured {@link SearchQuery} */
	private Query buildQuery(SearchQuery sq) throws Exception {

		BooleanQuery.Builder builder = new BooleanQuery.Builder();

		if (sq.bucketName != null && !sq.bucketName.isBlank())
			builder.add(new TermQuery(new Term(F_BUCKET, sq.bucketName)), BooleanClause.Occur.FILTER);

		/** free text over the catch-all field */
		if (sq.q != null && !sq.q.trim().isEmpty()) {
			QueryParser parser = new QueryParser(F_ALL, this.analyzer);
			parser.setAllowLeadingWildcard(false);
			builder.add(parser.parse(sq.q), BooleanClause.Occur.MUST);
		}

		/** objectName — exact ("..."), wildcard (* ?) or token match; case-insensitive */
		if (sq.objectName != null && !sq.objectName.isBlank())
			builder.add(nameQuery(F_OBJECT_KEY_TXT, F_OBJECT_KEY_LC, sq.objectName), BooleanClause.Occur.MUST);

		/** fileName — exact ("..."), wildcard (* ?) or token match; case-insensitive */
		if (sq.fileName != null && !sq.fileName.isBlank())
			builder.add(nameQuery(F_FILENAME_TXT, F_FILENAME_LC, sq.fileName), BooleanClause.Occur.MUST);

		/** lastModified inclusive range */
		if (sq.lastModifiedFrom != null || sq.lastModifiedTo != null) {
			long from = sq.lastModifiedFrom != null ? sq.lastModifiedFrom.toInstant().toEpochMilli() : Long.MIN_VALUE;
			long to = sq.lastModifiedTo != null ? sq.lastModifiedTo.toInstant().toEpochMilli() : Long.MAX_VALUE;
			builder.add(LongPoint.newRangeQuery(F_LAST_MODIFIED, from, to), BooleanClause.Occur.FILTER);
		}

		if (sq.metadata != null)
			for (Map.Entry<String, String> entry : sq.metadata.entrySet())
				builder.add(new TermQuery(new Term(F_META_PREFIX + entry.getKey(), entry.getValue())), BooleanClause.Occur.FILTER);

		BooleanQuery boolQuery = builder.build();
		return boolQuery.clauses().isEmpty() ? new MatchAllDocsQuery() : boolQuery;
	}

	/**
	 * <p>
	 * Name match with three semantics, all case-insensitive:
	 * </p>
	 * <ul>
	 * <li><b>exact</b> — value wrapped in double quotes ({@code "Report.pdf"}):
	 * {@link TermQuery} against the lowercased un-analyzed field</li>
	 * <li><b>wildcard / partial</b> — value contains {@code *} or {@code ?}
	 * ({@code *tolomei*}): {@link WildcardQuery} against the lowercased
	 * un-analyzed field. Leading wildcards are allowed (slow searches are
	 * tolerated)</li>
	 * <li><b>token</b> — otherwise: analyzed AND-match of tokens against the
	 * tokenized field</li>
	 * </ul>
	 */
	private Query nameQuery(String analyzedField, String lowercaseField, String value) throws IOException {

		String v = value.trim();

		/** exact: "name.pdf" -> case-insensitive exact match on the lowercased copy */
		if (v.length() > 1 && v.startsWith("\"") && v.endsWith("\""))
			return new TermQuery(new Term(lowercaseField, v.substring(1, v.length() - 1).toLowerCase()));

		/** wildcard / partial: case-insensitive on the lowercased copy */
		if (v.indexOf('*') >= 0 || v.indexOf('?') >= 0)
			return new WildcardQuery(new Term(lowercaseField, v.toLowerCase()));

		/** token match on the analyzed field */
		return fieldTextQuery(analyzedField, v);
	}

	/**
	 * <p>
	 * Builds an AND query of the analyzed tokens of {@code text} against a single
	 * tokenized field — e.g. {@code fieldTextQuery(F_FILENAME_TXT, "Tolomei
	 * Report")} matches documents whose file name contains both tokens
	 * {@code tolomei} and {@code report}, case-insensitively.
	 * </p>
	 */
	private Query fieldTextQuery(String field, String text) throws IOException {

		BooleanQuery.Builder builder = new BooleanQuery.Builder();

		try (TokenStream stream = this.analyzer.tokenStream(field, text)) {
			CharTermAttribute termAttr = stream.addAttribute(CharTermAttribute.class);
			stream.reset();
			while (stream.incrementToken())
				builder.add(new TermQuery(new Term(field, termAttr.toString())), BooleanClause.Occur.MUST);
			stream.end();
		}

		BooleanQuery q = builder.build();
		/** no tokens (e.g. only separators) -> match nothing */
		return q.clauses().isEmpty() ? new TermQuery(new Term(field, "\u0000")) : q;
	}

	@Override
	public IndexStatus getIndexStatus() {

		IndexStatus status = new IndexStatus();

		if (!this.serverSettings.isSearchEnabled()) {
			status.status = IndexStatus.DISABLED;
			return status;
		}

		if (getStatus() != ServiceStatus.RUNNING) {
			status.status = IndexStatus.DEGRADED;
			return status;
		}

		status.status = this.rebuilding ? IndexStatus.REBUILDING : IndexStatus.READY;
		status.errors = this.errorCount.get();

		try {
			this.searcherManager.maybeRefreshBlocking();
			IndexSearcher searcher = this.searcherManager.acquire();
			try {
				status.documents = searcher.getIndexReader().numDocs();
			} finally {
				this.searcherManager.release(searcher);
			}

			Map<String, String> userData = getCommitUserData();
			status.lastIndexed = parseDate(userData.get(UD_LAST_INDEXED));
			status.lastRebuild = parseDate(userData.get(UD_LAST_REBUILD));

		} catch (IOException e) {
			status.status = IndexStatus.DEGRADED;
			logger.error(e, SharedConstant.NOT_THROWN);
		}
		return status;
	}

	@Override
	public void markRebuildCompleted() {
		if (!isEnabled())
			return;
		try {
			Map<String, String> userData = new HashMap<String, String>(getCommitUserData());
			userData.put(UD_LAST_REBUILD, String.valueOf(System.currentTimeMillis()));
			userData.put(UD_SCHEMA_VERSION, SCHEMA_VERSION);
			userData.put(UD_ERROR_COUNT, String.valueOf(this.errorCount.get()));
			this.writer.setLiveCommitData(userData.entrySet());
			this.writer.commit();
		} catch (IOException e) {
			incrementErrorCount();
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	@Override
	public void incrementErrorCount() {
		this.errorCount.incrementAndGet();
	}

	@Override
	public void setRebuilding(boolean value) {
		this.rebuilding = value;
	}

	@Override
	public Map<String, String> getIndexedEtags(String bucketName) {

		Map<String, String> map = new HashMap<String, String>();

		if (!isEnabled())
			return map;

		try {
			this.searcherManager.maybeRefreshBlocking();
			IndexSearcher searcher = this.searcherManager.acquire();
			try {
				Query q = new TermQuery(new Term(F_BUCKET, bucketName));
				TopDocs top = searcher.search(q, Integer.MAX_VALUE);
				StoredFields storedFields = searcher.getIndexReader().storedFields();
				for (ScoreDoc sd : top.scoreDocs) {
					Document doc = storedFields.document(sd.doc);
					map.put(doc.get(F_OBJECT_KEY), nvl(doc.get(F_ETAG)));
				}
			} finally {
				this.searcherManager.release(searcher);
			}
		} catch (IOException e) {
			incrementErrorCount();
			throw new RuntimeException(e);
		}
		return map;
	}

	@Override
	public List<String> getIndexedBuckets() {

		List<String> buckets = new ArrayList<String>();

		if (!isEnabled())
			return buckets;

		try {
			this.searcherManager.maybeRefreshBlocking();
			IndexSearcher searcher = this.searcherManager.acquire();
			try {
				TopDocs top = searcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);
				StoredFields storedFields = searcher.getIndexReader().storedFields();
				for (ScoreDoc sd : top.scoreDocs) {
					String bucket = storedFields.document(sd.doc).get(F_BUCKET);
					if (bucket != null && !buckets.contains(bucket))
						buckets.add(bucket);
				}
			} finally {
				this.searcherManager.release(searcher);
			}
		} catch (IOException e) {
			incrementErrorCount();
			throw new RuntimeException(e);
		}
		return buckets;
	}

	/** ----------------------------------------------------------------- */

	private void commit(OffsetDateTime lastIndexed) throws IOException {
		Map<String, String> userData = new HashMap<String, String>(getCommitUserData());
		if (lastIndexed != null)
			userData.put(UD_LAST_INDEXED, String.valueOf(lastIndexed.toInstant().toEpochMilli()));
		userData.put(UD_SCHEMA_VERSION, SCHEMA_VERSION);
		userData.put(UD_ERROR_COUNT, String.valueOf(this.errorCount.get()));
		this.writer.setLiveCommitData(userData.entrySet());
		this.writer.commit();
	}

	private Map<String, String> getCommitUserData() throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		if (DirectoryReader.indexExists(this.directory)) {
			try (DirectoryReader reader = DirectoryReader.open(this.directory)) {
				Map<String, String> ud = reader.getIndexCommit().getUserData();
				if (ud != null)
					map.putAll(ud);
			}
		}
		return map;
	}

	private static String id(String bucketName, String objectName) {
		return bucketName + ":" + objectName;
	}

	private Document toDocument(ObjectMetadata meta) {

		Document doc = new Document();
		StringBuilder all = new StringBuilder();

		doc.add(new StringField(F_ID, id(meta.bucketName, meta.objectName), Field.Store.YES));
		doc.add(new StringField(F_BUCKET, nvl(meta.bucketName), Field.Store.YES));
		doc.add(new StringField(F_OBJECT_KEY, nvl(meta.objectName), Field.Store.YES));
		doc.add(new TextField(F_OBJECT_KEY_TXT, nvl(meta.objectName), Field.Store.NO));
		doc.add(new StringField(F_OBJECT_KEY_LC, nvl(meta.objectName).toLowerCase(), Field.Store.NO));
		all.append(nvl(meta.objectName)).append(" ");

		if (meta.fileName != null) {
			doc.add(new StoredField(F_FILENAME, meta.fileName));
			doc.add(new TextField(F_FILENAME_TXT, meta.fileName, Field.Store.NO));
			doc.add(new StringField(F_FILENAME_LC, meta.fileName.toLowerCase(), Field.Store.NO));
			all.append(meta.fileName).append(" ");
		}

		doc.add(new LongPoint(F_SIZE, meta.length));
		doc.add(new StoredField(F_SIZE, meta.length));

		doc.add(new StringField(F_CONTENT_TYPE, nvl(meta.contentType), Field.Store.YES));
		doc.add(new StringField(F_ETAG, nvl(meta.etag), Field.Store.YES));

		long lastModified = meta.lastModified != null ? meta.lastModified.toInstant().toEpochMilli() : (meta.creationDate != null ? meta.creationDate.toInstant().toEpochMilli() : 0L);
		doc.add(new LongPoint(F_LAST_MODIFIED, lastModified));
		doc.add(new StoredField(F_LAST_MODIFIED, lastModified));

		int metaFields = 0;

		if (meta.customTags != null) {
			for (String tag : meta.customTags) {
				if (tag == null || tag.isEmpty())
					continue;
				int sep = tag.indexOf(':');
				if (sep > 0 && sep < tag.length() - 1) {
					/** key:value entry -> dynamic per-key metadata field */
					String key = tag.substring(0, sep).trim();
					String value = tag.substring(sep + 1).trim();
					if (isIndexableMetadataKey(key) && metaFields < MAX_META_FIELDS) {
						doc.add(new StringField(F_META_PREFIX + key, value, Field.Store.NO));
						doc.add(new TextField(F_META_TXT_PREFIX + key, value, Field.Store.NO));
						all.append(value).append(" ");
						metaFields++;
					}
				}
				/** every tag is also searchable / displayable as a plain tag */
				doc.add(new StringField(F_TAGS, tag, Field.Store.YES));
				doc.add(new TextField(F_TAGS_TXT, tag, Field.Store.NO));
				all.append(tag).append(" ");
			}
		}

		if (meta.systemTags != null && !meta.systemTags.isEmpty()) {
			doc.add(new StringField(F_TAGS, meta.systemTags, Field.Store.YES));
			doc.add(new TextField(F_TAGS_TXT, meta.systemTags, Field.Store.NO));
			all.append(meta.systemTags).append(" ");
		}

		all.append(nvl(meta.bucketName)).append(" ").append(nvl(meta.contentType));
		doc.add(new TextField(F_ALL, all.toString(), Field.Store.NO));

		return doc;
	}

	/** rebuild a stored Document under a new bucket name (bucket rename) */
	private Document rekey(Document old, String newBucketName) {

		ObjectMetadata meta = new ObjectMetadata();
		meta.bucketName = newBucketName;
		meta.objectName = old.get(F_OBJECT_KEY);
		meta.fileName = old.get(F_FILENAME);
		meta.contentType = old.get(F_CONTENT_TYPE);
		meta.etag = old.get(F_ETAG);

		String size = old.get(F_SIZE);
		meta.length = size != null ? Long.parseLong(size) : 0;

		String lastModified = old.get(F_LAST_MODIFIED);
		if (lastModified != null)
			meta.lastModified = OffsetDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(lastModified)), ZoneId.systemDefault());

		String[] tags = old.getValues(F_TAGS);
		if (tags != null && tags.length > 0) {
			meta.customTags = new ArrayList<String>();
			for (String t : tags)
				meta.customTags.add(t);
		}
		return toDocument(meta);
	}

	private SearchResult toResult(Document doc, float score) {
		SearchResult result = new SearchResult();
		result.bucketName = doc.get(F_BUCKET);
		result.objectName = doc.get(F_OBJECT_KEY);
		result.fileName = doc.get(F_FILENAME);
		result.contentType = doc.get(F_CONTENT_TYPE);
		result.etag = doc.get(F_ETAG);
		result.score = score;

		String size = doc.get(F_SIZE);
		result.size = size != null ? Long.parseLong(size) : 0;

		String lastModified = doc.get(F_LAST_MODIFIED);
		if (lastModified != null)
			result.lastModified = OffsetDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(lastModified)), ZoneId.systemDefault());

		String[] tags = doc.getValues(F_TAGS);
		if (tags != null)
			for (String t : tags)
				result.tags.add(t);

		return result;
	}

	private boolean isIndexableMetadataKey(String key) {
		if (this.metadataKeyWhitelist.isEmpty())
			return true;
		return this.metadataKeyWhitelist.contains(key);
	}

	private static OffsetDateTime parseDate(String epochMillis) {
		if (epochMillis == null)
			return null;
		try {
			return OffsetDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(epochMillis)), ZoneId.systemDefault());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static String nvl(String s) {
		return s != null ? s : "";
	}
}
