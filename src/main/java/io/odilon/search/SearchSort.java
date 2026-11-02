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

/**
 * <p>
 * Server-side sort order of a {@link SearchQuery}. Sorting is applied by
 * Lucene <b>before</b> the {@code offset / maxResults} window is cut, so the
 * results are the true top-N for the selected order (e.g. the newest 1000
 * objects for {@link #DATE_DESC}).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public enum SearchSort {

	/** Lucene score (best match first) */
	RELEVANCE,

	/** lastModified, newest first */
	DATE_DESC,

	/** lastModified, oldest first */
	DATE_ASC,

	/** bucket name, then object name (case-insensitive) */
	BUCKET_OBJECT,

	/** object name (case-insensitive) */
	OBJECT_NAME,

	/** file name (case-insensitive) */
	FILE_NAME;

	/**
	 * Lenient parser for API parameters: case-insensitive, accepts {@code -} as
	 * {@code _}. Returns {@code null} for null / blank values.
	 * 
	 * @throws IllegalArgumentException for unknown values
	 */
	public static SearchSort parse(String value) {
		if (value == null || value.isBlank())
			return null;
		String v = value.trim().toUpperCase().replace('-', '_');
		for (SearchSort s : values())
			if (s.name().equals(v))
				return s;
		throw new IllegalArgumentException("invalid sort -> " + value);
	}
}
