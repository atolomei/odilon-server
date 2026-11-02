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

import java.util.ArrayList;
import java.util.List;

import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.markup.html.navigation.paging.AjaxPagingNavigator;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.PropertyModel;

import io.odilon.log.Logger;
import io.odilon.search.SearchQuery;
import io.odilon.search.SearchResult;
import io.odilon.search.SearchService;
import io.odilon.web.ServiceLocator;
import io.wktui.nav.menu.NavDropDownMenu;
import io.wktui.struct.list.ListPanel;
import io.wktui.struct.list.ListPanelMode;
import io.wktui.struct.list.ListPanelToolbar;

/**
 * <p>
 * Results of a search: a {@link ListPanel} with a {@link SearchResultsToolbar}
 * (sort selector on the left, page navigator and total on the right). Each
 * item displays the object name (prefixed by the bucket when searching all
 * buckets), opens the file inline in a new tab on click (presigned URL), and
 * expands to an {@link ObjectExpandedPanel} with the object metadata and
 * versions.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ResultsPanel extends Panel {

	private static final long serialVersionUID = 1L;

	static private Logger logger = Logger.getLogger(ResultsPanel.class.getName());

	private static final int PAGE_SIZE = 25;

	private final List<SearchResult> results;

	/** the query that produced the results — re-executed when the order changes */
	private final SearchQuery query;

	/** whether the search was over all buckets (title displays bucket / objectName) */
	private final boolean allBuckets;

	private OrderOption orderOption = OrderOption.getDefault();

	private WebMarkupContainer container;
	private ListPanel<SearchResult> panel;

	public ResultsPanel(String id, SearchQuery query, List<SearchResult> results, boolean allBuckets) {
		super(id);
		this.query = query;
		this.results = (results != null) ? new ArrayList<SearchResult>(results) : new ArrayList<SearchResult>();
		this.allBuckets = allBuckets;
		if (query != null)
			this.orderOption = OrderOption.of(query.getEffectiveSort());
		setOutputMarkupId(true);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		this.container = new WebMarkupContainer("container");
		this.container.setOutputMarkupId(true);
		add(this.container);

		this.panel = new ListPanel<SearchResult>("contents") {

			private static final long serialVersionUID = 1L;

			@Override
			public List<IModel<SearchResult>> getItems() {
				return ResultsPanel.this.getList();
			}

			@Override
			public Integer getTotalItems() {
				return Integer.valueOf(results.size());
			}

			@Override
			protected Panel getListItemPanel(IModel<SearchResult> model, ListPanelMode mode) {
				return new SearchResultItemPanel("row-element", model, allBuckets);
			}

			@Override
			protected WebMarkupContainer getListItemExpandedPanel(IModel<SearchResult> model, ListPanelMode mode) {
				return new ObjectExpandedPanel("expanded-panel", model);
			}

			@Override
			protected ListPanelToolbar newToolbar(AjaxPagingNavigator navigator, NavDropDownMenu<Void> settingsMenu) {
				return new SearchResultsToolbar("toolbar", navigator, settingsMenu) {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer getTotal() {
						return Integer.valueOf(results.size());
					}

					@Override
					protected IModel<OrderOption> getOrderModel() {
						return new PropertyModel<OrderOption>(ResultsPanel.this, "orderOption");
					}

					@Override
					protected void onOrderChange(AjaxRequestTarget target) {
						ResultsPanel.this.reload();
						target.add(ResultsPanel.this.container);
					}
				};
			}

			@Override
			public String getToolbarCss() {
				return "w-100 border rounded bg-body-tertiary float-start pt-1 pb-1 ps-3 pe-3 mb-2";
			}
		};

		this.panel.setBorder(false);
		this.panel.setHasExpander(true);
		this.panel.setSettings(false);
		this.panel.setLiveSearch(false);
		this.panel.setToolbarVisible(true);
		this.panel.setPageSize(PAGE_SIZE);
		this.panel.setListPanelMode(ListPanelMode.TITLE_TEXT);

		this.container.add(this.panel);
	}

	/** items of the ListPanel (rebuilt on every render, after sorting) */
	protected List<IModel<SearchResult>> getList() {
		List<IModel<SearchResult>> list = new ArrayList<IModel<SearchResult>>();
		this.results.forEach(r -> list.add(Model.of(r)));
		return list;
	}

	/**
	 * re-executes the query with the selected order. Sorting is done by Lucene
	 * before the 1000-results cut, so the list is the true top-N for that order
	 * (sorting the current 1000 in memory would miss e.g. the newest objects)
	 */
	private void reload() {
		try {
			if (this.query == null) {
				this.results.sort(this.orderOption.comparator());
				return;
			}
			this.query.sort = this.orderOption.toSearchSort();
			List<SearchResult> list = getSearchService().search(this.query);
			this.results.clear();
			if (list != null)
				this.results.addAll(list);
		} catch (Exception e) {
			logger.error(e);
		}
	}

	protected SearchService getSearchService() {
		return ServiceLocator.getInstance().getBean(SearchService.class);
	}
}