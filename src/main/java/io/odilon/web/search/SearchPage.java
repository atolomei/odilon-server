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

import java.util.List;

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.wicketstuff.annotation.mount.MountPath;

import io.odilon.log.Logger;
import io.odilon.search.SearchQuery;
import io.odilon.search.SearchResult;
import io.odilon.web.page.BasePage;
import io.wktui.error.ErrorPanel;
import wktui.base.InvisiblePanel;

/**
 * <p>
 * Search page: {@link SearchFormEditor} and {@link ResultsPanel}.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@MountPath("/ui/search")
public class SearchPage extends BasePage {

	private static final long serialVersionUID = 1L;

	static private Logger logger = Logger.getLogger(SearchPage.class.getName());

	/** container of the form and results panel (Ajax refresh target) */
	private WebMarkupContainer container;

	public SearchPage() {
		this(new PageParameters());
	}

	public SearchPage(PageParameters parameters) {
		super(parameters);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		this.container = new WebMarkupContainer("container");
		this.container.setOutputMarkupId(true);
		add(this.container);

		this.container.add(new SearchFormEditor("searchform") {
			private static final long serialVersionUID = 1L;

			@Override
			protected void onSearch(AjaxRequestTarget target, SearchQuery query, String bucket) {
				SearchPage.this.executeSearch(target, query, bucket == null);
			}
		});

		this.container.add(new InvisiblePanel("results"));
	}

	protected void executeSearch(AjaxRequestTarget target, SearchQuery query, boolean allBuckets) {

		try {
			List<SearchResult> results = getSearchService().search(query);
			this.container.addOrReplace(new ResultsPanel("results", results, allBuckets));

		} catch (Exception e) {
			logger.error(e);
			this.container.addOrReplace(new ErrorPanel("results", e));
		}

		if (target != null)
			target.add(this.container);
	}
}
