package com.alibaba.datax.insertcommon.exception;

import com.alibaba.datax.insertcommon.constant.ErrorConstant;

/**
 * @author zengwei
 * @date 2019-07-24 10:34
 */
public class NoInsertTypeException extends Exception {
    public NoInsertTypeException() {
        super(ErrorConstant.NO_INSERT_TYPE_ERROR);
    }
}
