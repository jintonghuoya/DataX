package com.alibaba.datax.insertcommon.writer.baseapi;

/**
 * 调用各接口返回的结果以泛型替代
 *
 * @author zengwei
 * @date 2019-07-24 17:50
 */
public class Result<T> {

    private T response;

    public T getResponse() {
        return response;
    }

    public void setResponse(T response) {
        this.response = response;
    }

    public Result(T response) {
        this.response = response;
    }
}
