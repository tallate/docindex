package com.tallate.docindex.util;

/**
 * @author hgc
 */
public class EsException extends RuntimeException {

    public EsException() {
    }

    public EsException(Throwable cause) {
        super(cause);
    }

    public EsException(String msg) {
        super(msg);
    }

    public EsException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
