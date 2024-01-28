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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class CronExpressionDeserializer extends StdDeserializer<CronExpressionJ8>  {

	private static final long serialVersionUID = 1L;

	public CronExpressionDeserializer() {
	     this(null);
	   }

	   public CronExpressionDeserializer(Class<?> vc) {
	     super(vc);
	   }

	   @Override
	   public CronExpressionJ8 deserialize(JsonParser parser, DeserializationContext ctx)
	       throws IOException, JacksonException {

		   JsonNode node = parser.getCodec().readTree(parser);
		   String expr = node.get("expr").asText();
		   
		   return new CronExpressionJ8(expr, true);
	   }
}


/**
Integer id = (Integer) ((IntNode) node.get("id")).numberValue();
String timestamp = node.get("timestamp").asText();
ArchiveStatus status = new ArchiveStatus(false);
if(node.get("status") != null) {
  String active = node.get("status").asText();
  if("active".equalsIgnoreCase(active)) {
    status.setActive(true);
  }
}
return new Record(id.longValue(), message, ZonedDateTime.parse(timestamp, dtf), status);
**/

