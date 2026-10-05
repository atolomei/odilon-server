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
package io.odilon.web.home;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.LoadableDetachableModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.wicketstuff.annotation.mount.MountPath;

import com.giffing.wicket.spring.boot.context.scan.WicketHomePage;

import io.odilon.log.Logger;
import io.odilon.model.RedundancyLevel;
import io.odilon.search.IndexStatus;
import io.odilon.search.SearchQuery;
import io.odilon.search.SearchResult;
import io.odilon.service.ServerSettings;
import io.odilon.web.page.BasePage;
import io.odilon.web.panel.KeyValuePanel;
import io.wktui.error.ErrorPanel;

/**
 * <p>
 * Dashboard: recent activity (newest 20 files uploaded), metrics panel and
 * search status panel.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@WicketHomePage
@MountPath("/ui")
public class HomePage extends BasePage {

	private static final long serialVersionUID = 1L;

	static private Logger logger = Logger.getLogger(HomePage.class.getName());

	private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	private static final int RECENT = 30;

	public HomePage() {
		this(new PageParameters());
	}

	public HomePage(PageParameters parameters) {
		super(parameters);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		/**
		try {
			add(new ListView<SearchResult>("recent", getRecentModel()) {
				private static final long serialVersionUID = 1L;

				@Override
				protected void populateItem(ListItem<SearchResult> item) {
					SearchResult r = item.getModelObject();
					ExternalLink link = new ExternalLink("link", presignedUrl(r.bucketName, r.objectName));
					link.add(new Label("name", r.bucketName + " / " + r.objectName));
					item.add(link);
					item.add(new Label("fileName", r.fileName));
					item.add(new Label("lastModified", r.lastModified != null ? FMT.format(r.lastModified) : "-"));
				}
			});
		} catch (Exception e) {
			logger.error(e);
			addOrReplace(new ErrorPanel("recent", e));
		}
		 **/
		
		/** Main configuration */
		add(new KeyValuePanel("mainconfig", getLabel("main-configuration"), new LoadableDetachableModel<List<KeyValuePanel.Row>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<KeyValuePanel.Row> load() {
				try {
					ServerSettings settings = getServerSettings();
					Map<String, String> map = new LinkedHashMap<String, String>();

					/** when EC -> include data and erasure disks */
					if (settings.getRedundancyLevel() == RedundancyLevel.ERASURE_CODING)
						map.put("Data Redundancy", "Erasure coding [" + settings.getECDataDrives() + "," + settings.getECParityDrives() + "]");
					else
						map.put("Data Redundancy", settings.getRedundancyLevel() != null ? settings.getRedundancyLevel().getName() : "-");

					map.put("Encryption", settings.isEncryptionEnabled() ? "enabled" : "disabled");
					map.put("Version Control", settings.getVersionControl() != null ? settings.getVersionControl().getName() : "-");
					map.put("Data immutability", settings.getDataStorage() != null ? settings.getDataStorage().getName() : "-");
					map.put("Master standby", settings.isStandByEnabled() ? "enabled" : "disabled");
					map.put("Server mode", settings.getServerMode());
					map.put("HTTPS", settings.isHTTPS() ? "yes" : "no");

					return KeyValuePanel.rows(map);
				} catch (Exception e) {
					logger.error(e);
					return new ArrayList<KeyValuePanel.Row>();
				}
			}
		}));

		/** Objects uploaded */
		add(new ObjectsUploadedPanel("objectsuploaded"));

		/** Metrics */
		add(new KeyValuePanel("metrics", getLabel("metrics"), new LoadableDetachableModel<List<KeyValuePanel.Row>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<KeyValuePanel.Row> load() {
				try {
					return KeyValuePanel.rows(getSystemMonitorService().getMetricsValues().getColloquial());
				} catch (Exception e) {
					logger.error(e);
					return new ArrayList<KeyValuePanel.Row>();
				}
			}
		}));


 
		add(new KeyValuePanel("info", getLabel("system-info"), new LoadableDetachableModel<List<KeyValuePanel.Row>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<KeyValuePanel.Row> load() {
				try {
					return KeyValuePanel.rows(getSystemInfoService().getSystemInfo().getColloquial());
				} catch (Exception e) {
					logger.error(e);
					return new ArrayList<KeyValuePanel.Row>();
				}
			}
		}));

		
		
		/** Search status */
		add(new KeyValuePanel("searchstatus", getLabel("search-status"), new LoadableDetachableModel<List<KeyValuePanel.Row>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<KeyValuePanel.Row> load() {
				try {
					IndexStatus status = getSearchService().getIndexStatus();
					Map<String, String> map = new LinkedHashMap<String, String>();
					map.put("Status", status.status);
					map.put("Documents", String.format("%,d", status.documents));
					map.put("Last indexed", status.lastIndexed != null ? FMT.format(status.lastIndexed) : "-");
					map.put("Last rebuild", status.lastRebuild != null ? FMT.format(status.lastRebuild) : "-");
					map.put("Pending", String.format("%,d", status.pending));
					map.put("Errors", String.format("%,d", status.errors));
					return KeyValuePanel.rows(map);
				} catch (Exception e) {
					logger.error(e);
					return new ArrayList<KeyValuePanel.Row>();
				}
			}
		}));
	}

	private IModel<List<SearchResult>> getRecentModel() {
		return new LoadableDetachableModel<List<SearchResult>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<SearchResult> load() {
				try {
					if (!getSearchService().isEnabled())
						return new ArrayList<SearchResult>();

					SearchQuery query = new SearchQuery();
					query.maxResults = 1000;
					List<SearchResult> results = new ArrayList<SearchResult>(getSearchService().search(query));
					results.sort(Comparator.comparing((SearchResult r) -> r.lastModified, Comparator.nullsLast(Comparator.reverseOrder())));
					return results.size() > RECENT ? new ArrayList<SearchResult>(results.subList(0, RECENT)) : results;
				} catch (Exception e) {
					logger.error(e);
					return new ArrayList<SearchResult>();
				}
			}
		};
	}
}
