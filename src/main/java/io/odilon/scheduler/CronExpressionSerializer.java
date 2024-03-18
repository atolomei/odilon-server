/*
 * Odilon Object Storage
 * (C) Novamens 
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

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class CronExpressionSerializer extends StdSerializer<CronExpressionJ8> {

	private static final long serialVersionUID = 1L;

	protected CronExpressionSerializer() {
		     super(CronExpressionJ8.class);
	}
		   
	@Override
	public void serialize(CronExpressionJ8 value, JsonGenerator gen,
		                         SerializerProvider serializers) throws IOException {

	   gen.writeStartObject();
	   gen.writeStringField("expr", value.getExpression());
	   gen.writeEndObject();
	}
}



/**
gen.writeNumberField("id", value.getId());
gen.writeStringField("message", value.getMessage());
gen.writeStringField("timestamp", dtf.format(value.getTimestamp()));
if (value.getStatus() != null) {
  gen.writeStringField("status", value.getStatus().getActive() ?
      "active" : "inactive");
}
**/

