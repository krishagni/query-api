package com.krishagni.query.services.impl;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;

import com.krishagni.commons.errors.AppException;
import com.krishagni.query.errors.QueryErrorCode;
import com.krishagni.query.events.ExecuteQueryOp;
import com.krishagni.query.events.QueryExecResult;
import com.krishagni.query.services.QueryService;

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
}
