package com.alibaba.datax.insertcommon.writer.baseapi;

import com.alibaba.datax.common.element.Record;

import java.sql.Connection;
import java.util.List;

/**
 * @author zengwei
 * @date 2019-07-24 18:04
 */
public class AnalyticDbOperate implements Operate<Connection> {
    @Override
    public Result doBatchInsert(List<Record> records, Connection connect) {
        return null;
    }

    @Override
    public List<Record> getExistsRows(List<Record> records, Connection connect) {
        return null;
    }

    @Override
    public Result truncate(String tableName, Connection connect) {
        return null;
    }

    @Override
    public Result updateRow(Record record, Connection connect) {
        return null;
    }
}
