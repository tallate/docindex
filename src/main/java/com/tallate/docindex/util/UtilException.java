package com.tallate.docindex.util;

/**
 * 普通异常类，在应用中调用时会强制要求捕捉
 *
 * @author hgc
 */
public class UtilException extends RuntimeException {

    public UtilException(String msg) {
        super(msg);
    }

    public UtilException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public UtilException(Throwable cause) {
        super(cause);
    }
}
