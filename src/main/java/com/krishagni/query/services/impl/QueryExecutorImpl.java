package com.krishagni.query.services.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
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
import com.krishagni.query.events.FieldDetail;
import com.krishagni.query.events.FilterDetail;
import com.krishagni.query.events.QueryExecResult;
import com.krishagni.query.services.QueryExecutor;
import com.krishagni.query.services.QueryExecutorConfig;

import edu.common.dynamicextensions.domain.nui.Container;
import edu.common.dynamicextensions.domain.nui.Control;
import edu.common.dynamicextensions.domain.nui.DataType;
import edu.common.dynamicextensions.domain.nui.LookupControl;
import edu.common.dynamicextensions.query.PathConfig;
import edu.common.dynamicextensions.query.Query;
import edu.common.dynamicextensions.query.QueryException;
import edu.common.dynamicextensions.query.QueryParserException;
import edu.common.dynamicextensions.query.QueryResponse;
import edu.common.dynamicextensions.query.QueryResultCsvExporter;
import edu.common.dynamicextensions.query.QueryResultData;
import edu.common.dynamicextensions.query.QueryResultExporter;
import edu.common.dynamicextensions.query.WideRowMode;

public class QueryExecutorImpl implements QueryExecutor, InitializingBean {
	private static final Log logger = LogFactory.getLog(QueryExecutorImpl.class);

	private QueryExecutorConfig config;

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
			queryResult.setScreener(config.getScreener(query));

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
	public void exportData(ExecuteQueryOp op, OutputStream out, char fieldSeparator, Consumer<Object> onFinish) {
		QueryResultExporter exporter = new QueryResultCsvExporter(fieldSeparator);
		try {
			Query query = getQuery(op);
			QueryResponse resp = exporter.export(out, query, config.getScreener(query));
			// insertAuditLog(user, opDetail, resp);
			onFinish.accept(true);
		} catch (Exception e) {
			logger.error("Error exporting query data", e);
			onFinish.accept(e);
		} finally {
			IOUtils.closeQuietly(out);
		}
	}

	@Override
	public List<FilterDetail> getParameterisedFilters(QueryDef query) {
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
	public FieldDetail getFieldValues(String fqn, String searchTerm, Map<String, Object> appData) {
		String[] fieldParts = fqn.split("\\.");

		String aqlFmt = "select distinct %s where %s %s limit 0, 500";
		String condition = StringUtils.isNotBlank(searchTerm) ? "contains \"" + searchTerm.trim() + "\"" : "exists";

		ExecuteQueryOp op = new ExecuteQueryOp();
		op.setAppData(appData);
		op.setAql(String.format(aqlFmt, fqn, fqn, condition));
		op.setDrivingForm(fieldParts[0]);
		op.setWideRowMode(WideRowMode.OFF.name());
		op.setRunType("Data");

		Query query = getQuery(op);
		QueryResponse queryResp = query.getData();
		QueryResultData queryResult = queryResp.getResultData();
		queryResult.setScreener(config.getScreener(query));

		Collection<Object> values = new TreeSet<>();
		for (Object[] row : queryResult.getRows()) {
			if (row[0] != null && !row[0].toString().isEmpty()) {
				values.add(row[0]);
			}
		}

		String[] columnLabels = queryResp.getResultData().getColumnLabels()[0].split("#");
		FieldDetail result = new FieldDetail();
		result.setFqn(fqn);
		result.setCaption(columnLabels[columnLabels.length - 1]);
		result.setValues(new ArrayList<>(values));
		return result;
	}

	@Override
	public void bindFilterValues(QueryDef query, List<FilterDetail> criteria) {
		Map<Integer, FilterDetail> criteriaMap = criteria.stream().collect(Collectors.toMap(f -> f.getId(), f -> f));

		for (Filter filter : query.getFilters()) {
			if (!filter.isParameterized()) {
				continue;
			}

			FilterDetail criterion = criteriaMap.get(filter.getId());
			if (criterion != null) {
				bindFilterValue(filter, criterion);
			}
		}
	}

	@Override
	public QueryExecutorConfig getConfig() {
		return config;
	}

	public void setConfig(QueryExecutorConfig config) {
		this.config = config;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		InputStream in = null;
		try {
			if (StringUtils.isBlank(config.getPathsConfig())) {
				return;
			}

			in = Thread.currentThread().getContextClassLoader().getResourceAsStream(config.getPathsConfig());
			PathConfig.initialize(in);
		} finally {
			IOUtils.closeQuietly(in);
		}
	}

	private Query getQuery(ExecuteQueryOp op) {
		config.addRestrictions(op);

		Query query = Query.createQuery()
			.wideRowMode(WideRowMode.valueOf(op.getWideRowMode()))
			.ic(true)
			.dateFormat(config.getDateFormat())
			.timeFormat(config.getTimeFormat());
		query.compile(op.getDrivingForm(), op.getAql(), op.getRestriction());
		return query;
	}

	private boolean incConcurrentQueriesCnt() {
		while (true) {
			int current = concurrentQueriesCnt.get();
			if (current >= config.getMaxConcurrentQueries()) {
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

	private FilterDetail getFacet(Map<String, Container> formCache, Filter filter) {
		FilterDetail facet = new FilterDetail();
		Integer facetId = filter.getId();
		facet.setId(facetId);
		facet.setName(facetId.toString());

		DataType dataType;
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

		return form.getControlByUdn(fieldName, "\\.");
	}

	private List<Object> getPreConfiguredValues(Filter filter) {
		String[] values = filter.getValues();
		if (values == null || values.length == 0 || (values.length == 1 && values[0] == null)) {
			return null;
		}

		return Arrays.stream(values).collect(Collectors.toList());
	}

	private void bindFilterValue(Filter filter, FilterDetail criterion) {
		if (StringUtils.isBlank(criterion.getSearchType()) || CollectionUtils.isEmpty(criterion.getValues())) {
			return;
		}

		if ("RANGE".equals(criterion.getSearchType())) {
			bindRangeCondition(filter, criterion);
		} else if ("EQUALS".equals(criterion.getSearchType())) {
			bindEqualsCondition(filter, criterion);
		}
	}

	private void bindRangeCondition(Filter filter, FilterDetail criterion) {
		if (StringUtils.isBlank(filter.getExpr())) {
			bindFieldRangeCondition(filter, criterion);
		} else {
			bindExprRangeCondition(filter, criterion);
		}
	}

	private void bindFieldRangeCondition(Filter filter, FilterDetail criterion) {
		Object minValue = criterion.getValues().get(0);
		Object maxValue = criterion.getValues().get(1);

		if (minValue == null && maxValue != null) {
			filter.setOp(Filter.Op.LE);
			filter.setValues(new String[] {maxValue.toString()});
		} else if (minValue != null && maxValue == null) {
			filter.setOp(Filter.Op.GE);
			filter.setValues(new String[] {minValue.toString()});
		} else if (minValue != null && maxValue != null) {
			filter.setOp(Filter.Op.BETWEEN);
			filter.setValues(new String[] {minValue.toString(), maxValue.toString()});
		}
	}

	private void bindExprRangeCondition(Filter filter, FilterDetail criterion) {
		Object minValue = criterion.getValues().get(0);
		Object maxValue = criterion.getValues().get(1);

		String expr = filter.getExpr();
		if (minValue == null && maxValue != null) {
			filter.setExpr(getLhs(expr) + " <= " + maxValue.toString());
		} else if (minValue != null && maxValue == null) {
			filter.setExpr(getLhs(expr) + " >= " + minValue.toString());
		} else if (minValue != null && maxValue != null) {
			filter.setExpr(getLhs(expr) + " between (" + minValue.toString() + ", " + maxValue.toString() + ")");
		}
	}

	private String getLhs(String expr) {
		String[] lhsRhs = expr.split("[<=>!]|\\sany\\s*$|\\sexists\\s*$|\\snot exists\\s*$|\\sbetween\\s");
		return lhsRhs[0];
	}

	private void bindEqualsCondition(Filter filter, FilterDetail criterion) {
		filter.setOp(Filter.Op.IN);

		String[] values = new String[criterion.getValues().size()];
		int idx = 0;
		for (Object val : criterion.getValues()) {
			values[idx++] = val.toString();
		}

		filter.setValues(values);
	}
}
