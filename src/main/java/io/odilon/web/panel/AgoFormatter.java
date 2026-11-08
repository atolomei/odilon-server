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
package io.odilon.web.panel;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * <p>
 * Formats a date as "d MMM yyyy, HH:mm:ss (xxx ago)".
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class AgoFormatter {

	private static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("d MMM yyyy, HH:mm:ss");

	public static String format(OffsetDateTime date) {
		return (date == null) ? "-" : format(date.toInstant());
	}

	public static String format(Instant instant) {
		if (instant == null)
			return "-";
		return DTF.format(instant.atZone(ZoneId.systemDefault())) + " (" + elapsed(Duration.between(instant, Instant.now())) + " ago)";
	}

	public static String elapsed(Duration d) {
		if (d.isNegative())
			d = Duration.ZERO;
		long days = d.toDays();
		long h = d.toHours() % 24;
		long m = d.toMinutes() % 60;
		if (days > 0)
			return days + " days " + h + " h";
		if (h > 0)
			return h + " h " + m + " min";
		if (m > 0)
			return m + " min";
		return d.getSeconds() + " s";
	}
}
