package com.alibaba.datax.insertcommon.factory;

import com.alibaba.datax.insertcommon.constant.DBEnum;
import com.alibaba.datax.insertcommon.constant.InsertEnum;
import com.alibaba.datax.insertcommon.writer.*;
import com.alibaba.datax.insertcommon.writer.baseapi.AnalyticDbOperate;
import com.alibaba.datax.insertcommon.writer.baseapi.TableStoreOperate;

import java.util.HashMap;

/**
 * @author zengwei
 * @date 2019-07-24 15:58
 */
public class WriterFactory {

    /**
     * 类关系均在此处注册，通过数据库和插入方式列表可获取类实例
     */
    private final static HashMap<DBEnum, HashMap<InsertEnum, AbstractWriter>> writerRegister =
            new HashMap<DBEnum, HashMap<InsertEnum, AbstractWriter>>() {{
                put(DBEnum.ANALYTIC_DB, new HashMap<InsertEnum, AbstractWriter>() {{
                    put(InsertEnum.TWM01, new AnalyticDBTWM01(new AnalyticDbOperate()));
                    put(InsertEnum.TWM02, new AnalyticDBTWM02(new AnalyticDbOperate()));
                    put(InsertEnum.TWM04, new AnalyticDBTWM04(new AnalyticDbOperate()));
                    put(InsertEnum.TWM05, new AnalyticDBTWM05(new AnalyticDbOperate()));
                }});

                put(DBEnum.TABLE_STORE, new HashMap<InsertEnum, AbstractWriter>() {{
                    put(InsertEnum.TWM01, new TableStoreTWM01(new TableStoreOperate()));
                    put(InsertEnum.TWM02, new TableStoreTWM02(new TableStoreOperate()));
                    put(InsertEnum.TWM04, new TableStoreTWM04(new TableStoreOperate()));
                    put(InsertEnum.TWM05, new TableStoreTWM05(new TableStoreOperate()));
                }});
            }};

    /**
     * @param db         数据库源 table_store|analytic_db
     * @param insertType 写入类别
     * @return writer实例
     * @throws Exception
     */
    public static AbstractWriter getWriter(String db, String insertType) throws Exception {

        DBEnum dbEnum = DBEnum.getDBEnum(db.toUpperCase());

        InsertEnum insertEnum = InsertEnum.getInsertEnum(insertType.toUpperCase());

        return writerRegister.get(dbEnum).get(insertEnum);
    }
}
