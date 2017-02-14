package com.krishagni.query.domain;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;

public class QueryDef {
	private String drivingForm;

	private Filter[] filters;

	private ExpressionNode[] expressionNodes;

	private Object[] selectList;

	private ReportSpec reporting;

	public String getDrivingForm() {
		return drivingForm;
	}

	public void setDrivingForm(String drivingForm) {
		this.drivingForm = drivingForm;
	}

	public Filter[] getFilters() {
		return filters;
	}

	public void setFilters(Filter[] filters) {
		this.filters = filters;
	}

	public ExpressionNode[] getExpressionNodes() {
		return expressionNodes;
	}

	public void setExpressionNodes(ExpressionNode[] expressionNodes) {
		this.expressionNodes = expressionNodes;
	}

	public Object[] getSelectList() {
		return selectList;
	}

	public void setSelectList(Object[] selectList) {
		this.selectList = selectList;
	}

	public ReportSpec getReporting() {
		return reporting;
	}

	public void setReporting(ReportSpec reporting) {
		this.reporting = reporting;
	}

	public String getAql() {
		return AqlBuilder.getInstance().getQuery(selectList, filters, expressionNodes);
	}

	public String getAql(Filter[] conjunctionFilters) {
		return AqlBuilder.getInstance().getQuery(selectList, filters, conjunctionFilters, expressionNodes);
	}

	public QueryDef copy() {
		return QueryDef.fromJson(toJson());
	}

	public String toJson() {
		try {
			return getWriteMapper().writeValueAsString(this);
		} catch (Exception e) {
			throw new RuntimeException("Error serialising query def to JSON", e);
		}
	}

	public static QueryDef fromJson(String queryDefJson) {
		QueryDef queryDef = null;
		try {
			queryDef = getReadMapper().readValue(queryDefJson, QueryDef.class);
		} catch (Exception e) {
			throw new RuntimeException("Error deserialising JSON to query def", e);
		}

		queryDef.selectList = curateSelectList(queryDef.selectList);
		return queryDef;
	}


	private static ObjectMapper getReadMapper() {
		return new ObjectMapper();
	}

	private static ObjectMapper getWriteMapper() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.setVisibilityChecker(
			mapper.getSerializationConfig().getDefaultVisibilityChecker()
				.withFieldVisibility(JsonAutoDetect.Visibility.ANY)
				.withGetterVisibility(JsonAutoDetect.Visibility.NONE)
				.withSetterVisibility(JsonAutoDetect.Visibility.NONE)
				.withCreatorVisibility(JsonAutoDetect.Visibility.NONE)
		);
		return mapper;
	}

	private static Object[] curateSelectList(Object[] selectList) {
		Object[] result = new Object[selectList.length];
		int idx = 0;
		for (Object field : selectList) {
			if (field instanceof Map) {
				field = new ObjectMapper().convertValue(field, SelectField.class);
			}

			result[idx++] = field;
		}

		return result;
	}
}
