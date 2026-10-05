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

import java.util.ArrayList;
import java.util.List;

import org.apache.wicket.model.LoadableDetachableModel;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.wicketstuff.annotation.mount.MountPath;

import io.odilon.log.Logger;
import io.odilon.web.page.BasePage;
import io.odilon.web.panel.KeyValuePanel;

/**
 * <p>
 * Info page: System info and System metrics panels.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@MountPath("/ui/info")
public class InfoPage extends BasePage {

	private static final long serialVersionUID = 1L;

	static private Logger logger = Logger.getLogger(InfoPage.class.getName());

	public InfoPage() {
		this(new PageParameters());
	}

	public InfoPage(PageParameters parameters) {
		super(parameters);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		add(new KeyValuePanel("systeminfo", getLabel("system-info"), new LoadableDetachableModel<List<KeyValuePanel.Row>>() {
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

		add(new KeyValuePanel("systemmetrics", getLabel("system-metrics"), new LoadableDetachableModel<List<KeyValuePanel.Row>>() {
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
	}
}
