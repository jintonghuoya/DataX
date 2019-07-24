package com.alibaba.datax.insertcommon.writer.baseapi;

import com.alibaba.datax.common.element.Record;

import java.util.List;

/**
 * @author zengwei
 * @date 2019-07-24 17:27
 */
public interface Operate<C> {

    /**
     * 执行批量导入
     *
     * @param records 需要导入的记录
     * @param connect 连接|客户端实例等
     * @return
     */
    Result doBatchInsert(List<Record> records, C connect);

    /**
     * @param records
     * @param connect 连接|客户端实例
     * @return
     */
    List<Record> getExistsRows(List<Record> records, C connect);

    /**
     * @param tableName
     * @param connect   连接|客户端实例等
     * @return
     */
    Result truncate(String tableName, C connect);

    /**
     * @param record
     * @param connect 连接|客户端实例等
     * @return
     */
    Result updateRow(Record record, C connect);
}
