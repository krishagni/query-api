package com.krishagni.query.services;

import java.util.List;

import com.krishagni.query.domain.QueryDef;
import com.krishagni.query.events.ExecuteQueryOp;
import com.krishagni.query.events.FacetDetail;
import com.krishagni.query.events.QueryExecResult;

public interface QueryService {
	QueryExecResult executeQuery(ExecuteQueryOp op);

	List<FacetDetail> getFacets(QueryDef query);
}
