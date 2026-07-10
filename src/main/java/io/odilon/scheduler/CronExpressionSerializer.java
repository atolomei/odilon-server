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
package io.odilon.scheduler;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ser.std.StdSerializer;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class CronExpressionSerializer extends StdSerializer<CronExpressionJ8> {

	// private static final long serialVersionUID = 1L;

	public CronExpressionSerializer() {
		super(CronExpressionJ8.class);
	}

	@Override
	public void serialize(CronExpressionJ8 value, JsonGenerator gen, SerializationContext ctx) {

		gen.writeStartObject();
		gen.writeString(value.getExpression());
		gen.writeEndObject();
	}
}