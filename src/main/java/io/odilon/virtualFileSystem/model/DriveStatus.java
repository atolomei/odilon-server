package io.odilon.virtualFileSystem.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import io.odilon.util.Check;

public enum DriveStatus {

	NOTSYNC("notsync", 1), ENABLED("enabled", 2), ARCHIVED("archived", 3), DELETED("deleted", 4);

	static List<DriveStatus> ops;

	private String name;
	private int code;

	private DriveStatus(String name, int code) {
		this.name = name;
		this.code = code;
	}

	public String getDescription() {
		return getDescription(Locale.getDefault());
	}

	public String getDescription(Locale locale) {
		return this.getName();
	}

	/**
	 * Backward compatible with:
	 *
	 * OLD Jackson 2 JSON: { "name": "notsync", "code": 1 }
	 *
	 * NEW Jackson 3 JSON: "notsync"
	 */
	@JsonCreator
	public static DriveStatus fromValue(String value) {

		if (value == null) {
			return null;
		}

		String normalized = value.trim();

		/*
		 * Legacy Jackson 2 format:
		 *
		 * DriveStatus{"name": "notsync", "code": 1}
		 */
		if (normalized.startsWith("DriveStatus{")) {

			int idx = normalized.indexOf("\"name\":");

			if (idx >= 0) {

				int firstQuote = normalized.indexOf('"', idx + 7);
				int secondQuote = normalized.indexOf('"', firstQuote + 1);

				if (firstQuote >= 0 && secondQuote > firstQuote) {

					normalized = normalized.substring(firstQuote + 1, secondQuote);
				}
			}
		}

		/*
		 * Modern format: "notsync"
		 *
		 * Also supports enum constant names: "NOTSYNC"
		 */
		for (DriveStatus status : DriveStatus.values()) {

			if (status.name.equalsIgnoreCase(normalized) || status.name().equalsIgnoreCase(normalized)) {

				return status;
			}
		}

		throw new IllegalArgumentException("Unknown status value: " + value);
	}

	/**
	 * New serialization format: "enabled"
	 */
	@JsonValue
	public String toJson() {
		return this.name;
	}

	public String toJSON() {

		StringBuilder str = new StringBuilder();

		str.append("\"name\": \"" + name + "\"");
		str.append(", \"code\": " + String.valueOf(code));

		return str.toString();
	}

	@Override
	public String toString() {

		StringBuilder str = new StringBuilder();

		str.append(this.getClass().getSimpleName() + "{");
		str.append(toJSON());
		str.append("}");

		return str.toString();
	}

	public String getName() {
		return name;
	}

	public int getCode() {
		return code;
	}

	public static DriveStatus fromId(String id) {

		Check.requireNonNullStringArgument(id, "id is null or empty");

		try {

			int value = Integer.valueOf(id).intValue();

			return get(value);

		} catch (IllegalArgumentException e) {

			throw (e);

		} catch (Exception e) {

			throw new IllegalArgumentException("id can not be converted int Integer -> " + id);
		}
	}

	public static List<DriveStatus> getValues() {

		if (ops != null)
			return ops;

		ops = new ArrayList<DriveStatus>();

		ops.add(NOTSYNC);
		ops.add(ENABLED);
		ops.add(ARCHIVED);
		ops.add(DELETED);

		return ops;
	}

	public static DriveStatus get(int code) {

		if (code == NOTSYNC.getCode())
			return NOTSYNC;

		if (code == ENABLED.getCode())
			return ENABLED;

		if (code == ARCHIVED.getCode())
			return ARCHIVED;

		if (code == DELETED.getCode())
			return DELETED;

		throw new IllegalArgumentException("unsupported code -> " + String.valueOf(code));
	}
}