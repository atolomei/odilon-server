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
package io.odilon.web.info;

import java.io.File;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.wicket.extensions.ajax.markup.html.AjaxLazyLoadPanel;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.LoadableDetachableModel;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.wicketstuff.annotation.mount.MountPath;

import io.odilon.log.Logger;
import io.odilon.web.page.BasePage;
import io.odilon.web.panel.AgoFormatter;
import io.odilon.web.panel.KeyValuePanel;

/**
 * <p>
 * Server info page: hardware, OS, JVM and status of the server, plus the system
 * metrics.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@MountPath("/ui/info")
public class ServerInfoPage extends BasePage {

	private static final long serialVersionUID = 1L;

	static private Logger logger = Logger.getLogger(ServerInfoPage.class.getName());

	private static final double GB = 1024.0 * 1024.0 * 1024.0;

	

	public ServerInfoPage() {
		this(new PageParameters());
	}

	public ServerInfoPage(PageParameters parameters) {
		super(parameters);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		addPanel("hardware", "hardware-os", () -> hardware());
		addPanel("status", "status", () -> status());
		addPanel("resources", "system-resources", () -> resources());
		addPanel("jvm", "jvm", () -> jvm());

		/** potentially slow: loaded lazily (ajax) after the page is rendered */
		add(new AjaxLazyLoadPanel<Panel>("systemmetrics") {
			private static final long serialVersionUID = 1L;

			@Override
			public Panel getLazyLoadComponent(String markupId) {
				return newPanel(markupId, "system-metrics", () -> getSystemMonitorService().getMetricsValues().getColloquial());
			}
		});
	}

	/** Wicket serializes pages: lambdas captured by components must be Serializable */
	@FunctionalInterface
	private interface InfoSupplier extends Supplier<Map<String, String>>, Serializable {
	}

	private void addPanel(String id, String labelKey, InfoSupplier supplier) {
		add(newPanel(id, labelKey, supplier));
	}

	private Panel newPanel(String id, String labelKey, InfoSupplier supplier) {
		return new KeyValuePanel(id, getLabel(labelKey), new LoadableDetachableModel<List<KeyValuePanel.Row>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<KeyValuePanel.Row> load() {
				try {
					return KeyValuePanel.rows(supplier.get());
				} catch (Exception e) {
					logger.error(e);
					Map<String, String> map = new LinkedHashMap<String, String>();
					map.put("Error", e.getClass().getCanonicalName() + ": " + e.getMessage());
					return KeyValuePanel.rows(map);
				}
			}
		});
	}

	private Map<String, String> hardware() {
		Map<String, String> map = new LinkedHashMap<String, String>();
		Runtime rt = Runtime.getRuntime();

		map.put("Available processors", rt.availableProcessors() + " cores");
		map.put("Free memory", gb(rt.freeMemory()));
		map.put("Maximum memory", gb(rt.maxMemory()));
		map.put("Total memory", gb(rt.totalMemory()));

		File[] roots = File.listRoots();
		if (roots != null) {
			int n = 1;
			for (File f : roots)
				map.put("FS root " + (n++), f.getAbsolutePath() + " (Total: " + gb(f.getTotalSpace()) + ", Free: " + gb(f.getFreeSpace()) + ")");
		}

		try {
			Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
			int n = 1;
			while (nis != null && nis.hasMoreElements()) {
				NetworkInterface ni = nis.nextElement();
				if (!ni.isUp())
					continue;
				Enumeration<InetAddress> addrs = ni.getInetAddresses();
				while (addrs.hasMoreElements()) {
					InetAddress a = addrs.nextElement();
					/** no getHostName(): it triggers a reverse DNS lookup (slow) */
					map.put("Inet Address " + (n++) + " (" + ni.getName() + ")", a.getHostAddress());
				}
			}
		} catch (Exception e) {
			map.put("Inet Address", "-");
		}

		map.put("JVM Spec", nz(System.getProperty("java.specification.version")));
		map.put("OS", nz(System.getProperty("os.name")));
		map.put("OS Version", nz(System.getProperty("os.version")));
		map.put("OS Arch", nz(System.getProperty("os.arch")));
		map.put("Profile", nz(System.getProperty("spring.profiles.active")));
		map.put("Server", nz(System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : System.getenv("COMPUTERNAME")));
		map.put("user.country", nz(System.getProperty("user.country")));
		map.put("user.dir", nz(System.getProperty("user.dir")));
		map.put("user.home", nz(System.getProperty("user.home")));
		map.put("user.language", nz(System.getProperty("user.language")));
		return map;
	}

	private Map<String, String> status() {
		Map<String, String> map = new LinkedHashMap<String, String>();
		RuntimeMXBean rmx = ManagementFactory.getRuntimeMXBean();
		Instant start = Instant.ofEpochMilli(rmx.getStartTime());
		map.put("JVM Started", DTF.format(start.atZone(ZoneId.systemDefault())) + " (" + elapsed(Duration.between(start, Instant.now())) + " ago)");

		String app = "-", sys = "-";
		try {
			OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
			if (os instanceof com.sun.management.OperatingSystemMXBean) {
				com.sun.management.OperatingSystemMXBean sun = (com.sun.management.OperatingSystemMXBean) os;
				double p = sun.getProcessCpuLoad();
				double s = sun.getCpuLoad();
				if (p >= 0)
					app = String.format("%.2f%%", p * 100.0);
				if (s >= 0)
					sys = String.format("%.2f%%", s * 100.0);
			}
		} catch (Throwable t) {
			/* unsupported */
		}
		map.put("App CPU usage", app);
		map.put("System CPU usage", sys);
		return map;
	}

	private Map<String, String> resources() {
		Map<String, String> map = new LinkedHashMap<String, String>();
		OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

		double load = os.getSystemLoadAverage();
		map.put("System load average", load >= 0 ? String.format("%.2f", load) : "-");

		String phys = "-", swap = "-", fd = "-";
		try {
			if (os instanceof com.sun.management.OperatingSystemMXBean) {
				com.sun.management.OperatingSystemMXBean sun = (com.sun.management.OperatingSystemMXBean) os;
				long total = sun.getTotalMemorySize();
				long free = sun.getFreeMemorySize();
				phys = pct(total - free, total) + " (" + gb(total) + " total, " + gb(total - free) + " used)";
				long st = sun.getTotalSwapSpaceSize();
				long sf = sun.getFreeSwapSpaceSize();
				swap = pct(st - sf, st) + " (" + gb(st) + " total, " + gb(st - sf) + " used)";
			}
		} catch (Throwable t) {
			/* unsupported */
		}
		try {
			if (os instanceof com.sun.management.UnixOperatingSystemMXBean) {
				com.sun.management.UnixOperatingSystemMXBean u = (com.sun.management.UnixOperatingSystemMXBean) os;
				long open = u.getOpenFileDescriptorCount();
				long max = u.getMaxFileDescriptorCount();
				fd = pct(open, max) + " (max: " + max + ", open: " + open + ")";
			}
		} catch (Throwable t) {
			/* unsupported */
		}
		map.put("Physical Memory", phys);
		map.put("Swap Space", swap);
		map.put("File Descriptor Count", fd);
		return map;
	}

	private Map<String, String> jvm() {
		Map<String, String> map = new LinkedHashMap<String, String>();
		RuntimeMXBean rmx = ManagementFactory.getRuntimeMXBean();
		map.put("JVM Runtime", nz(System.getProperty("java.vendor")) + " " + nz(rmx.getVmName()) + " " + nz(System.getProperty("java.version")) + " " + nz(rmx.getVmVersion()));
		map.put("JVM Args", String.join(" ", rmx.getInputArguments()));
		return map;
	}

	private static String gb(long bytes) {
		return String.format("%.4f GB", bytes / GB);
	}

	private static String pct(long part, long total) {
		return total > 0 ? String.format("%.1f%%", part * 100.0 / total) : "-";
	}

	private static String nz(String s) {
		return (s == null || s.isEmpty()) ? "-" : s;
	}

	
}
