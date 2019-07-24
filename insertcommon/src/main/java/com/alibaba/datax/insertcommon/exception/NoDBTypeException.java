package com.alibaba.datax.insertcommon.exception;

import com.alibaba.datax.insertcommon.constant.ErrorConstant;

/**
 * @author zengwei
 * @date 2019-07-24 10:42
 */
public class NoDBTypeException extends Exception {
    public NoDBTypeException() {
        super(ErrorConstant.NO_DB_TYPE_ERROR);
    }
}
