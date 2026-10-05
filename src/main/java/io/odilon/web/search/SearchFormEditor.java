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
package io.odilon.web.search;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.markup.html.form.AjaxButton;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.LoadableDetachableModel;
import org.apache.wicket.model.PropertyModel;

import io.odilon.log.Logger;
import io.odilon.search.SearchQuery;
import io.odilon.service.ObjectStorageService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.web.ServiceLocator;
import wktui.base.ModelPanel;

/**
 * <p>
 * Search form: bucket choice (all buckets plus "All"), object name, file name,
 * modified from / to and a Search button. On submit it builds a
 * {@link SearchQuery} and calls {@link #onSearch(AjaxRequestTarget, SearchQuery, String)}.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class SearchFormEditor extends ModelPanel<Void> {

	private static final long serialVersionUID = 1L;

	static private Logger logger = Logger.getLogger(SearchFormEditor.class.getName());

	public static final String ALL_BUCKETS = "All";

	/** form fields */
	private String bucket = ALL_BUCKETS;
	private String objectName;
	private String fileName;

	/** html5 date inputs (yyyy-MM-dd) */
	private String modifiedFrom;
	private String modifiedTo;

	public SearchFormEditor(String id) {
		super(id, null);
		setOutputMarkupId(true);
	}

	/** called on submit with the structured query; bucket is null when "All" */
	protected abstract void onSearch(AjaxRequestTarget target, SearchQuery query, String bucket);

	@Override
	public void onInitialize() {
		super.onInitialize();

		Form<Void> form = new Form<Void>("searchForm");
		form.setOutputMarkupId(true);
		add(form);

		form.add(new DropDownChoice<String>("bucket", new PropertyModel<String>(this, "bucket"), new LoadableDetachableModel<List<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<String> load() {
				List<String> buckets = new ArrayList<String>();
				buckets.add(ALL_BUCKETS);
				try {
					
					List<ServerBucket> list = getObjectStorageService().findAllBuckets();
					list.sort((a, b) -> a.getName().compareToIgnoreCase(b.getName()));
					list.forEach(b -> buckets.add(b.getName()));
				} catch (Exception e) {
					logger.error(e);
				}
				return buckets;
			}
		}));

		form.add(new TextField<String>("objectName", new PropertyModel<String>(this, "objectName")));
		form.add(new TextField<String>("fileName", new PropertyModel<String>(this, "fileName")));
		form.add(newDateField("modifiedFrom", new PropertyModel<String>(this, "modifiedFrom")));
		form.add(newDateField("modifiedTo", new PropertyModel<String>(this, "modifiedTo")));

		form.add(new AjaxButton("search", form) {
			private static final long serialVersionUID = 1L;

			@Override
			protected void onSubmit(AjaxRequestTarget target) {
				SearchFormEditor.this.onSearch(target, buildQuery(), isAllBuckets() ? null : bucket);
			}

			@Override
			protected void onError(AjaxRequestTarget target) {
				target.add(SearchFormEditor.this);
			}
		});
	}

	public boolean isAllBuckets() {
		return bucket == null || bucket.isBlank() || ALL_BUCKETS.equals(bucket);
	}

	/** TextField bound to an html5 {@code <input type="date">} */
	private TextField<String> newDateField(String id, PropertyModel<String> model) {
		return new TextField<String>(id, model) {
			private static final long serialVersionUID = 1L;

			@Override
			protected String[] getInputTypes() {
				return new String[] { "date" };
			}
		};
	}

	private SearchQuery buildQuery() {

		SearchQuery query = new SearchQuery();

		if (!isAllBuckets())
			query.bucketName = bucket;

		query.objectName = blankToNull(objectName);
		query.fileName = blankToNull(fileName);

		ZoneId zone = ZoneId.systemDefault();

		try {
			if (blankToNull(modifiedFrom) != null)
				query.lastModifiedFrom = LocalDate.parse(modifiedFrom.trim()).atStartOfDay(zone).toOffsetDateTime();
		} catch (Exception e) {
			logger.error(e, "invalid date -> " + modifiedFrom);
		}

		try {
			if (blankToNull(modifiedTo) != null)
				query.lastModifiedTo = LocalDate.parse(modifiedTo.trim()).plusDays(1).atStartOfDay(zone).minusNanos(1).toOffsetDateTime();
		} catch (Exception e) {
			logger.error(e, "invalid date -> " + modifiedTo);
		}

		/** only the 1000 most relevant */
		query.maxResults = 1000;
		query.offset = 0;

		return query;
	}

	private static String blankToNull(String s) {
		return (s == null || s.isBlank()) ? null : s.trim();
	}

	protected ObjectStorageService getObjectStorageService() {
		return ServiceLocator.getInstance().getBean(ObjectStorageService.class);
	}
}
