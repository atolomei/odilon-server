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
package io.odilon.traffic;

import java.io.Serializable;
import java.util.Optional;

/**
 * <p>
 * Tokens used by the services that control the incoming flow of requests in the
 * API
 * </p>
 * 
 * {@see TrafficControlService}
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 * 
 */

public interface TrafficPass extends Serializable {
	
	  public void setCaller( String caller );
	  public Optional<String> getCaller();
	  
}
