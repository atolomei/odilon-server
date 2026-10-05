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

/**
 * Sort options of the results toolbar. The results (max 1000) are sorted
 * in memory.
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
