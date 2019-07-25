package com.alibaba.datax.insertcommon.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.insertcommon.exception.MethodNotImplement;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量写入必然有缓冲区
 * flush方法必须在结束时再执行，防止缓冲区数据未导入
 *
 * @author zengwei
 * @date 2019-07-24 14:00
 */
public abstract class AbstractWriter<C> {

    private int batchRecordLimit = 100;

    private List<Record> recordBuffer = new ArrayList<>();

    /**
     * 连接或客户端
     */
    private C client;

    public C getClient() {
        return client;
    }

    public void setClient(C client) {
        this.client = client;
    }

    /**
     * 在初始化时执行，如清空表, 或执行preSql
     */
    public void init() {

    }

    /**
     * 单条记录写入
     * todo 批量写入出错会怎么样？ mysql兼容方案是采用单条再次写入
     * 去除数据库本身的原因的话，mysql批量是可能是主键冲突等原因， 通过单条写入可以避免，tableStore和analyticDB是否有这样的问题。
     *
     * @param record
     */
    public void insert(Record record) throws Exception {

        recordBuffer.add(record);

        if (recordBuffer.size() >= batchRecordLimit) {

            batchInsert(recordBuffer);

            recordBuffer.clear();
        }
    }

    /**
     * 在此处实现业务所需逻辑
     * 还需要数据转换， 接口对接, 具体业务逻辑
     *
     * @param records 写入的数据记录
     * @throws Exception 有问题抛出异常，正常导入则无异常
     */
    protected <T> void batchInsert(List<T> records) throws Exception {
        throw new MethodNotImplement();
    }

    /**
     * 执行缓冲区中存量数据，在finally执行
     */
    public void flush() throws Exception {

        if (recordBuffer.size() > 0) {

            batchInsert(recordBuffer);

            recordBuffer.clear();
        }
    }
}
