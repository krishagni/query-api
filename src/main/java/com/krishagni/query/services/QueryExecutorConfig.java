package com.krishagni.query.services;

import edu.common.dynamicextensions.query.Query;
import edu.common.dynamicextensions.query.QueryResultScreener;

public interface QueryExecutorConfig {
	int getMaxConcurrentQueries();

	String getDateFormat();

	String getTimeFormat();

	String getPathsConfig();

	QueryResultScreener getScreener(Query query);
}
