package com.krishagni.query.services;

import com.krishagni.query.events.ExecuteQueryOp;

import edu.common.dynamicextensions.query.Query;
import edu.common.dynamicextensions.query.QueryResultScreener;

public interface QueryExecutorConfig {
	int getMaxConcurrentQueries();

	String getDateFormat();

	String getTimeFormat();

	String getPathsConfig();

	void addRestrictions(ExecuteQueryOp op);

	QueryResultScreener getScreener(Query query);
}
