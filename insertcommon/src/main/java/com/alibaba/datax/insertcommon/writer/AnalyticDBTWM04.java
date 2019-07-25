package com.alibaba.datax.insertcommon.writer;

import com.alibaba.cloud.analyticdb.adbclient.AdbClient;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.insertcommon.writer.baseapi.AnalyticDbOperate;

import java.util.List;

/**
 * analyticdb 导入
 * 导入逻辑:{@link com.alibaba.datax.insertcommon.constant.InsertEnum#TWM04}
 *
 * @author zengwei
 * @date 2019-07-24 14:49
 */
public class AnalyticDBTWM04 extends AbstractWriter<AdbClient> {

    private AnalyticDbOperate analyticDbOperate;

    public AnalyticDBTWM04(AnalyticDbOperate analyticDbOperate) {
        this.analyticDbOperate = analyticDbOperate;
    }
}
