package com.aliyun.odps.cupid.presto;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.INTERNAL_ERROR;
import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum OdpsErrorCode implements ErrorCodeSupplier {
	ODPS_INTERNAL_ERROR(0, INTERNAL_ERROR), ODPS_AMBIGUOUS_OBJECT_NAME(1, EXTERNAL);
	/* Shared error code with HiveErrorCode */;

	public static final int ERROR_CODE_MASK = 0x0100_0000;

	private final ErrorCode errorCode;

	OdpsErrorCode(int code, ErrorType type) {
		errorCode = new ErrorCode(code + ERROR_CODE_MASK, name(), type);
	}

	@Override
	public ErrorCode toErrorCode() {
		return errorCode;
	}
}
