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
package io.odilon.web.search;

import java.util.Comparator;

import io.odilon.search.SearchResult;
import io.odilon.search.SearchSort;

/**
 * Sort options of the results toolbar. Sorting is done server-side by Lucene
 * (see {@link #toSearchSort()}), so the results are the true top 1000 for the
 * selected order. {@link #comparator()} is kept for in-memory sorting.
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public enum OrderOption {

	DATE_DESC("Date desc"), DATE_ASC("Date asc"), BUCKET_OBJECT("Bucket + Object name"), OBJECT_NAME("Object name"), FILE_NAME("File name");

	private final String label;

	OrderOption(String label) {
		this.label = label;
	}

	public String getLabel() {
		return label;
	}

	public static OrderOption getDefault() {
		return DATE_DESC;
	}

	/** server-side (Lucene) sort equivalent of this option */
	public SearchSort toSearchSort() {
		switch (this) {
		case DATE_ASC:
			return SearchSort.DATE_ASC;
		case BUCKET_OBJECT:
			return SearchSort.BUCKET_OBJECT;
		case OBJECT_NAME:
			return SearchSort.OBJECT_NAME;
		case FILE_NAME:
			return SearchSort.FILE_NAME;
		case DATE_DESC:
		default:
			return SearchSort.DATE_DESC;
		}
	}

	/** option for a server-side sort ({@link #getDefault()} for relevance / null) */
	public static OrderOption of(SearchSort sort) {
		if (sort == null)
			return getDefault();
		switch (sort) {
		case DATE_ASC:
			return DATE_ASC;
		case BUCKET_OBJECT:
			return BUCKET_OBJECT;
		case OBJECT_NAME:
			return OBJECT_NAME;
		case FILE_NAME:
			return FILE_NAME;
		default:
			return DATE_DESC;
		}
	}

	public Comparator<SearchResult> comparator() {
		switch (this) {
		case DATE_ASC:
			return Comparator.comparing((SearchResult r) -> r.lastModified, Comparator.nullsLast(Comparator.naturalOrder()));
		case BUCKET_OBJECT:
			return Comparator.comparing((SearchResult r) -> (nn(r.bucketName) + "/" + nn(r.objectName)).toLowerCase());
		case OBJECT_NAME:
			return Comparator.comparing((SearchResult r) -> nn(r.objectName).toLowerCase());
		case FILE_NAME:
			return Comparator.comparing((SearchResult r) -> nn(r.fileName).toLowerCase());
		case DATE_DESC:
		default:
			return Comparator.comparing((SearchResult r) -> r.lastModified, Comparator.nullsLast(Comparator.reverseOrder()));
		}
	}

	private static String nn(String s) {
		return s != null ? s : "";
	}
}
