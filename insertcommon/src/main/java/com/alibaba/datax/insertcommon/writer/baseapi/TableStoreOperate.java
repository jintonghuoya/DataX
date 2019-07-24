package com.alibaba.datax.insertcommon.writer.baseapi;

import com.alibaba.datax.common.element.Record;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.PutRowResponse;

import java.util.List;

/**
 * @author zengwei
 * @date 2019-07-24 18:02
 */
public class TableStoreOperate implements Operate<SyncClient> {
    @Override
    public Result<PutRowResponse> doBatchInsert(List<Record> records, SyncClient connect) {

        // todo demo
        PutRowResponse putRowResponse = connect.putRow(new PutRowRequest());

        return new Result(putRowResponse);
    }

    @Override
    public List<Record> getExistsRows(List<Record> records, SyncClient connect) {
        return null;
    }

    @Override
    public Result truncate(String tableName, SyncClient connect) {
        return null;
    }

    @Override
    public Result updateRow(Record record, SyncClient connect) {
        return null;
    }
}
