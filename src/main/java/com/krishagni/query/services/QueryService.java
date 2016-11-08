package com.krishagni.query.services;

import com.krishagni.query.events.ExecuteQueryOp;
import com.krishagni.query.events.QueryExecResult;

public interface QueryService {
	QueryExecResult executeQuery(ExecuteQueryOp op);
}
