package com.krishagni.query;

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
}
