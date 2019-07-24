package com.alibaba.datax.insertcommon.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.insertcommon.writer.baseapi.TableStoreOperate;
import com.alicloud.openservices.tablestore.SyncClient;

import java.util.List;

/**
 * 导入逻辑:{@link com.alibaba.datax.insertcommon.constant.InsertEnum#TWM05}
 *
 * @author zengwei
 * @date 2019-07-24 14:58
 */
public class TableStoreTWM05 extends AbstractWriter<SyncClient> {

    private TableStoreOperate tableStoreOperate;

    public TableStoreTWM05(TableStoreOperate tableStoreOperate) {
        this.tableStoreOperate = tableStoreOperate;
    }

    @Override
    public void batchInsert(List<Record> records) throws Exception {

    }
}
