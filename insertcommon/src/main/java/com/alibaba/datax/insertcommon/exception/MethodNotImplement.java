package com.alibaba.datax.insertcommon.exception;

import com.alibaba.datax.insertcommon.constant.ErrorConstant;

/**
 * @author zengwei
 * @date 2019-07-24 16:20
 */
public class MethodNotImplement extends Exception {
    public MethodNotImplement() {
        super(ErrorConstant.NO_METHOD_IMPLEMENT_ERROR);
    }
}
