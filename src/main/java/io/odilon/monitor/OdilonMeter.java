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
package io.odilon.monitor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * <p>
 * Event meter backed by a Micrometer {@link Counter}. Keeps the {@code mark()}
 * API formerly provided by Dropwizard. It only counts events; per-second rates
 * are computed by Prometheus/Grafana ({@code rate(odilon_..._total[1m])}).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class OdilonMeter {

	private final Counter counter;

	public OdilonMeter(MeterRegistry registry, String name, String description) {
		this.counter = Counter.builder(name).description(description).register(registry);
	}

	public void mark() {
		counter.increment();
	}

	public void mark(long n) {
		counter.increment(n);
	}

	public long getCount() {
		return (long) counter.count();
	}
}
