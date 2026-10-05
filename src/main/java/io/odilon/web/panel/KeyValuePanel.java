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
package io.odilon.web.panel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;

/**
 * <p>
 * Simple Bootstrap card that renders a title and a key / value table. Used by
 * the dashboard (metrics, search status) and the Info page (system info,
 * system metrics).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class KeyValuePanel extends Panel {

	private static final long serialVersionUID = 1L;

	/** a single row of the table */
	public static class Row implements Serializable {
		private static final long serialVersionUID = 1L;
		public final String key;
		public final String value;

		public Row(String key, String value) {
			this.key = key;
			this.value = value;
		}
	}

	private final IModel<String> title;
	private final IModel<List<Row>> rows;

	public KeyValuePanel(String id, IModel<String> title, IModel<List<Row>> rows) {
		super(id);
		this.title = title;
		this.rows = rows;
		setOutputMarkupId(true);
	}

	/** convenience: build the row list from a (linked) map, preserving iteration order */
	public static List<Row> rows(Map<String, String> map) {
		List<Row> list = new ArrayList<Row>();
		if (map != null)
			map.forEach((k, v) -> list.add(new Row(k, v)));
		return list;
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		add(new Label("title", this.title));

		add(new ListView<Row>("rows", this.rows) {
			private static final long serialVersionUID = 1L;

			@Override
			protected void populateItem(ListItem<Row> item) {
				item.add(new Label("key", item.getModelObject().key));
				item.add(new Label("value", item.getModelObject().value));
			}
		});
	}

	@Override
	public void onDetach() {
		super.onDetach();
		if (this.rows != null)
			this.rows.detach();
		if (this.title != null)
			this.title.detach();
	}
}
