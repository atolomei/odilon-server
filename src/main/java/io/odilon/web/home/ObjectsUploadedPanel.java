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

import java.io.Serializable;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.AjaxSelfUpdatingTimerBehavior;
import org.apache.wicket.ajax.form.AjaxFormComponentUpdatingBehavior;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.LoadableDetachableModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.PropertyModel;

import io.odilon.log.Logger;
import io.odilon.monitor.ObjectsUploaded;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.web.ServiceLocator;

/**
 * <p>
 * Objects uploaded per bucket: a bucket selector ("All" + all indexed buckets)
 * and the totals per time range, plus the datetime the count was metered. The
 * values come from {@link SystemMonitorService#getObjectsUploaded()} (cached,
 * not real time).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ObjectsUploadedPanel extends Panel {

	private static final long serialVersionUID = 1L;

	static private Logger logger = Logger.getLogger(ObjectsUploadedPanel.class.getName());

	private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	/** a single row of the totals table */
	public static class Row implements Serializable {
		private static final long serialVersionUID = 1L;
		public final String label;
		public final String value;

		public Row(String label, long value) {
			this.label = label;
			this.value = String.format("%,d", value);
		}
	}

	/** selected bucket ("All" by default) */
	private String bucket = ObjectsUploaded.ALL;

	private WebMarkupContainer container;

	public ObjectsUploadedPanel(String id) {
		super(id);
		setOutputMarkupId(true);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		this.container = new WebMarkupContainer("container");
		this.container.setOutputMarkupId(true);
		add(this.container);

		/** poll until the first snapshot is computed (background count at startup) */
		this.container.add(new AjaxSelfUpdatingTimerBehavior(Duration.ofSeconds(5)) {
			private static final long serialVersionUID = 1L;

			@Override
			protected void onPostProcessTarget(AjaxRequestTarget target) {
				/** stop polling once the first snapshot arrived */
				if (getSystemMonitorService().getObjectsUploaded().getMeasured() != null)
					stop(target);
			}
		});

		/** bucket selector: All + indexed buckets */
		DropDownChoice<String> selector = new DropDownChoice<String>("bucket", new PropertyModel<String>(this, "bucket"), new LoadableDetachableModel<List<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<String> load() {
				List<String> list = new ArrayList<String>();
				try {
					list.addAll(getSystemMonitorService().getObjectsUploaded().getBuckets().keySet());
				} catch (Exception e) {
					logger.error(e);
				}
				if (list.isEmpty())
					list.add(ObjectsUploaded.ALL);
				return list;
			}
		});

		selector.add(new AjaxFormComponentUpdatingBehavior("change") {
			private static final long serialVersionUID = 1L;

			@Override
			protected void onUpdate(AjaxRequestTarget target) {
				target.add(container);
			}
		});
		this.container.add(selector);

		/** totals of the selected bucket */
		this.container.add(new ListView<Row>("rows", new LoadableDetachableModel<List<Row>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<Row> load() {
				List<Row> rows = new ArrayList<Row>();
				try {
					ObjectsUploaded.Counts c = getSystemMonitorService().getObjectsUploaded().get(bucket);
					if (c == null)
						return rows;

					rows.add(new Row("1 min", c.lastMinute));
					rows.add(new Row("1 hour", c.lastHour));
					rows.add(new Row("Today", c.today));
					rows.add(new Row("Yesterday", c.yesterday));
					rows.add(new Row("1 Month", c.last30Days));
					rows.add(new Row("1 Year", c.last12Months));
					rows.add(new Row("All time", c.allTime));

				} catch (Exception e) {
					logger.error(e);
				}
				return rows;
			}
		}) {
			private static final long serialVersionUID = 1L;

			@Override
			protected void populateItem(ListItem<Row> item) {
				item.add(new Label("label", item.getModelObject().label));
				item.add(new Label("value", item.getModelObject().value));
			}
		});

		/** datetime the count was metered */
		this.container.add(new Label("metered", new Model<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String getObject() {
				try {
					java.time.OffsetDateTime measured = getSystemMonitorService().getObjectsUploaded().getMeasured();
					return (measured != null) ? FMT.format(measured) : getString("collecting");
				} catch (Exception e) {
					logger.error(e);
					return "-";
				}
			}
		}));
	}

	protected SystemMonitorService getSystemMonitorService() {
		return ServiceLocator.getInstance().getBean(SystemMonitorService.class);
	}
}
