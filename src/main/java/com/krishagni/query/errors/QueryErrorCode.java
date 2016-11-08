package com.krishagni.query.errors;

import com.krishagni.commons.errors.ErrorCode;

public enum  QueryErrorCode implements ErrorCode {
	TOO_BUSY,

	MALFORMED,

	OP_NOT_ALLOWED;

	@Override
	public String code() {
		return "QUERY_" + name();
	}
}
