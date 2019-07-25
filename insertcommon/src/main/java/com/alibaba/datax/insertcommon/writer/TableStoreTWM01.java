package com.alibaba.datax.insertcommon.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.insertcommon.writer.baseapi.Result;
import com.alibaba.datax.insertcommon.writer.baseapi.TableStoreOperate;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.RowChange;

import java.util.List;

/**
 * table store 导入
 * 导入逻辑:{@link com.alibaba.datax.insertcommon.constant.InsertEnum#TWM01}
 *
 * @author zengwei
 * @date 2019-07-24 14:49
 */
public class TableStoreTWM01 extends AbstractWriter<SyncClient> {

    private TableStoreOperate tableStoreOperate;

    public TableStoreTWM01(TableStoreOperate tableStoreOperate) {
        this.tableStoreOperate = tableStoreOperate;
    }

//    @Override
//    public void batchInsert(List<Record> records) throws Exception {
//
//        List<Record> existsRows = tableStoreOperate.getExistsRows(records, getClient());
//
//        Result<PutRowResponse> putRowResponseResult = tableStoreOperate.doBatchInsert(existsRows, getClient());
//    }

    @Override
    protected <T> void batchInsert(List<T> records) throws Exception {




    }
}
