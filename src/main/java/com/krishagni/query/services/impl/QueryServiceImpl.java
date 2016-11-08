package com.krishagni.query.services.impl;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;

import com.krishagni.commons.errors.AppException;
import com.krishagni.query.domain.Filter;
import com.krishagni.query.domain.QueryDef;
import com.krishagni.query.errors.QueryErrorCode;
import com.krishagni.query.events.ExecuteQueryOp;
import com.krishagni.query.events.FacetDetail;
import com.krishagni.query.events.QueryExecResult;
import com.krishagni.query.services.QueryService;

import edu.common.dynamicextensions.domain.nui.Container;
import edu.common.dynamicextensions.domain.nui.Control;
import edu.common.dynamicextensions.domain.nui.DataType;
import edu.common.dynamicextensions.domain.nui.LookupControl;
import edu.common.dynamicextensions.query.PathConfig;
import edu.common.dynamicextensions.query.Query;
import edu.common.dynamicextensions.query.QueryException;
import edu.common.dynamicextensions.query.QueryParserException;
import edu.common.dynamicextensions.query.QueryResponse;
import edu.common.dynamicextensions.query.QueryResultData;
import edu.common.dynamicextensions.query.WideRowMode;

public class QueryServiceImpl implements QueryService, InitializingBean {
	private static final Log logger = LogFactory.getLog(QueryServiceImpl.class);

	private int maxConcurrentQueries = 10;

	private String queryPathsCfg;

	private String dateFmt = "dd-MM-yyyy";

	private String timeFmt = "HH:mm";

	private AtomicInteger concurrentQueriesCnt = new AtomicInteger(0);

	@Override
	public QueryExecResult executeQuery(ExecuteQueryOp op) {
		QueryResultData queryResult = null;

		boolean queryCntIncremented = false;
		try {
			queryCntIncremented = incConcurrentQueriesCnt();

			Query query = getQuery(op);

			QueryResponse resp = query.getData();

			queryResult = resp.getResultData();

			Integer[] indices = null;
			if (op.getIndexOf() != null && !op.getIndexOf().isEmpty()) {
				indices = queryResult.getColumnIndices(op.getIndexOf());
			}

			return new QueryExecResult()
					.setColumnLabels(queryResult.getColumnLabels())
					.setColumnUrls(queryResult.getColumnUrls())
					.setRows(queryResult.getStringifiedRows())
					.setDbRowsCount(queryResult.getDbRowsCount())
					.setColumnIndices(indices);
		} catch (QueryParserException qpe) {
			throw AppException.userError(QueryErrorCode.MALFORMED, qpe.getMessage());
		} catch (QueryException qe) {
			throw AppException.userError(QueryErrorCode.MALFORMED, qe.getMessage());
		} catch (IllegalArgumentException iae) {
			throw AppException.userError(QueryErrorCode.MALFORMED, iae.getMessage());
		} catch (IllegalAccessError iae) {
			throw AppException.userError(QueryErrorCode.OP_NOT_ALLOWED, iae.getMessage());
		} catch (Exception e) {
			return AppException.raiseError(e);
		} finally {
			if (queryCntIncremented) {
				decConcurrentQueriesCnt();
			}

			if (queryResult != null) {
				try {
					queryResult.close();
				} catch (Exception e) {
					logger.error("Error closing query result stream", e);
				}
			}
		}
	}

	@Override
	public List<FacetDetail> getFacets(QueryDef query) {
		Map<String, Container> formCache = new HashMap<>();
		try {
			return Arrays.stream(query.getFilters())
				.filter(filter -> filter.isParameterized() && filter.getOp() != null)
				.map(filter -> getFacet(formCache, filter))
				.collect(Collectors.toList());
		} catch (Exception e) {
			return AppException.raiseError(e);
		} finally {
			formCache.clear();
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		InputStream in = null;
		try {
			if (StringUtils.isBlank(queryPathsCfg)) {
				return;
			}

			in = Thread.currentThread().getContextClassLoader().getResourceAsStream(queryPathsCfg);
			PathConfig.initialize(in);
		} finally {
			IOUtils.closeQuietly(in);
		}
	}

	public void setMaxConcurrentQueries(int maxConcurrentQueries) {
		this.maxConcurrentQueries = maxConcurrentQueries;
	}

	public void setQueryPathsCfg(String queryPathsCfg) {
		this.queryPathsCfg = queryPathsCfg;
	}

	private boolean incConcurrentQueriesCnt() {
		while (true) {
			int current = concurrentQueriesCnt.get();
			if (current >= maxConcurrentQueries) {
				throw AppException.userError(QueryErrorCode.TOO_BUSY);
			}

			if (concurrentQueriesCnt.compareAndSet(current, current + 1)) {
				break;
			}
		}

		return true;
	}

	private int decConcurrentQueriesCnt() {
		return concurrentQueriesCnt.decrementAndGet();
	}

	private Query getQuery(ExecuteQueryOp op) {
		Query query = Query.createQuery()
			.wideRowMode(WideRowMode.valueOf(op.getWideRowMode()))
			.ic(true)
			.dateFormat(dateFmt)
			.timeFormat(timeFmt);
		query.compile(op.getDrivingForm(), op.getAql(), op.getRestriction());
		return query;
	}

	private FacetDetail getFacet(Map<String, Container> formCache, Filter filter) {
		FacetDetail facet = new FacetDetail();
		Integer facetId = filter.getId();
		facet.setId(facetId.longValue());
		facet.setName(facetId.toString());

		DataType dataType = null;
		if (StringUtils.isNotBlank(filter.getExpr())) {
			dataType = DataType.INTEGER;
			facet.setCaption(filter.getDesc());
		} else {
			Control field = getField(formCache, filter.getField());
			if (field instanceof LookupControl) {
				dataType = ((LookupControl) field).getValueType();
			} else {
				dataType = field.getDataType();
			}
			facet.setCaption(field.getCaption());
		}

		facet.setDataType(dataType.name());
		switch (dataType) {
			case INTEGER:
			case FLOAT:
			case DATE:
				facet.setSearchType("RANGE");
				break;

			case STRING:
				facet.setSearchType("EQUALS");
				facet.setValues(getPreConfiguredValues(filter));
				break;
		}

		return facet;
	}

	private Control getField(Map<String, Container> formCache, String fieldExpr) {
		String[] fieldParts = fieldExpr.split("\\.");

		String formName, fieldName;
		if (fieldParts[1].equals("extensions") || fieldParts[1].equals("customFields")) {
			if (fieldParts.length < 4) { // <form>.[extensions|customFields].<form>.<fields...>
				//
				// should never occur
				//
				throw new RuntimeException("Invalid field expression: " + fieldExpr);
			}

			formName = fieldParts[2];
			fieldName = StringUtils.join(fieldParts, ".", 3, fieldParts.length);
		} else {
			formName = fieldParts[0];
			fieldName = StringUtils.join(fieldParts, ".", 1, fieldParts.length);
		}

		Container form = formCache.get(formName);
		if (form == null) {
			form = Container.getContainer(formName);
			if (form == null) {
				throw new RuntimeException("Error loading form : " + formName);
			}

			formCache.put(formName, form);
		}

		return form.getControl(fieldName, "\\.");
	}

	private List<Object> getPreConfiguredValues(Filter filter) {
		String[] values = filter.getValues();
		if (values == null || values.length == 0 || (values.length == 1 && values[0] == null)) {
			return null;
		}

		return Arrays.asList(values);
	}
}
