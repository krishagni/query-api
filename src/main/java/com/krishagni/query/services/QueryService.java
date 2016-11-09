package com.krishagni.query.services;

import java.util.List;

import com.krishagni.query.domain.QueryDef;
import com.krishagni.query.events.ExecuteQueryOp;
import com.krishagni.query.events.FilterDetail;
import com.krishagni.query.events.QueryExecResult;

public interface QueryService {
	QueryExecResult executeQuery(ExecuteQueryOp op);

	List<FilterDetail> getParameterisedFilters(QueryDef query);

	void bindFilterValues(QueryDef query, List<FilterDetail> criteria);
}
