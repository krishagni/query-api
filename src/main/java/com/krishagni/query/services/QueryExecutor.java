package com.krishagni.query.services;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.krishagni.query.domain.QueryDef;
import com.krishagni.query.events.ExecuteQueryOp;
import com.krishagni.query.events.FieldDetail;
import com.krishagni.query.events.FilterDetail;
import com.krishagni.query.events.QueryExecResult;

import edu.common.dynamicextensions.domain.nui.Control;

public interface QueryExecutor {
	QueryExecResult executeQuery(ExecuteQueryOp op);

	void exportData(ExecuteQueryOp op, OutputStream out, char fieldSeparator, Consumer<Object> onFinish);

	List<FilterDetail> getParameterisedFilters(QueryDef query);

	void bindFilterValues(QueryDef query, List<FilterDetail> criteria);

	FieldDetail getFieldValues(String fqn, String searchTerm, Map<String, Object> appData);

	QueryExecutorConfig getConfig();
}
