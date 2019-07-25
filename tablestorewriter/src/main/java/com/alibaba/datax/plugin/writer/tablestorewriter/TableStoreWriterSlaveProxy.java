package com.alibaba.datax.plugin.writer.tablestorewriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.insertcommon.constant.Constant;
import com.alibaba.datax.plugin.writer.tablestorewriter.model.*;
import com.alibaba.datax.plugin.writer.tablestorewriter.utils.Common;
import com.alibaba.datax.plugin.writer.tablestorewriter.utils.GsonParser;
import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.TableStoreWriter;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermsQuery;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.google.common.collect.Lists;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;


public class TableStoreWriterSlaveProxy {

    private static final Logger LOG = LoggerFactory.getLogger(TableStoreWriterSlaveProxy.class);
    private TableStoreConfig tableStoreConfig;
    private AsyncClient asyncClient;
    private SyncClient syncClient;
    private TableStoreWriter tableStoreWriter;

    private class WriterCallback implements TableStoreCallback<RowChange, ConsumedCapacity> {

        private TaskPluginCollector collector;

        public WriterCallback(TaskPluginCollector collector) {
            this.collector = collector;
        }

        @Override
        public void onCompleted(RowChange req, ConsumedCapacity res) {
            LOG.debug("Write row succeed. PrimaryKey: {}.", req.getPrimaryKey());
        }

        @Override
        public void onFailed(RowChange req, Exception ex) {
            LOG.error("Write row failed.", ex);
        }
    }

    public void init(Configuration configuration) {
        tableStoreConfig = GsonParser.jsonToConf(configuration.getString(TableStoreConst.TABLE_STORE_CONFIG));

        ClientConfiguration clientConfigure = new ClientConfiguration();
        clientConfigure.setIoThreadCount(tableStoreConfig.getIoThreadCount());
        clientConfigure.setMaxConnections(tableStoreConfig.getConcurrencyWrite());
        clientConfigure.setSocketTimeoutInMillisecond(tableStoreConfig.getSocketTimeout());
        clientConfigure.setConnectionTimeoutInMillisecond(tableStoreConfig.getConnectTimeout());
        clientConfigure.setRetryStrategy(new AlwaysRetryStrategy());

        asyncClient = new AsyncClient(
                tableStoreConfig.getEndpoint(),
                tableStoreConfig.getAccessId(),
                tableStoreConfig.getAccessKey(),
                tableStoreConfig.getInstanceName(),
                clientConfigure);

        syncClient = new SyncClient(
                tableStoreConfig.getEndpoint(),
                tableStoreConfig.getAccessId(),
                tableStoreConfig.getAccessKey(),
                tableStoreConfig.getInstanceName()
        );
    }

    public void close() {
        asyncClient.shutdown();
    }

    /**
     * 写Pooled Table的通用处理方式
     *
     * @param recordReceiver
     * @param collector
     * @throws Exception
     */
    public void write(RecordReceiver recordReceiver, TaskPluginCollector collector) throws Exception {
        LOG.info("Writer slave started.");

        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setConcurrency(tableStoreConfig.getConcurrencyWrite());
        writerConfig.setMaxBatchRowsCount(tableStoreConfig.getBatchWriteCount());
        writerConfig.setMaxBatchSize(tableStoreConfig.getRestrictConfig().getRequestTotalSizeLimitation());
        writerConfig.setBufferSize(tableStoreConfig.getBufferSize());
        writerConfig.setMaxAttrColumnSize(tableStoreConfig.getRestrictConfig().getAttributeColumnSize());
        writerConfig.setMaxColumnsCount(tableStoreConfig.getRestrictConfig().getMaxColumnsCount());
        writerConfig.setMaxPKColumnSize(tableStoreConfig.getRestrictConfig().getPrimaryKeyColumnSize());
        tableStoreWriter = new DefaultTableStoreWriter(asyncClient, tableStoreConfig.getTableName(), writerConfig,
                new WriterCallback(collector), Executors.newFixedThreadPool(3));

        List<Record> bufferList = new ArrayList<Record>();
        int bufferListLimit = 100;

        Record record;
        while ((record = recordReceiver.getFromReader()) != null) {
            LOG.debug("Record Raw: {}", record.toString());

            bufferList.add(record);

            if (bufferList.size() >= bufferListLimit) {
                dispatchInsert(bufferList, collector);

//                for (Record bufferRecord : bufferList) {
//                    // 类型转换
//                    try {
//                        RowChange rowChange = getRowChange(bufferRecord, tableStoreConfig);
//                        WithRecord withRecord = (WithRecord) rowChange;
//                        withRecord.setRecord(bufferRecord);
//
//                        tableStoreWriter.addRowChange(rowChange);
//
//                    } catch (IllegalArgumentException e) {
//                        LOG.warn("Found dirty data.", e);
//                        collector.collectDirtyRecord(record, e.getMessage());
//                    } catch (ClientException e) {
//                        LOG.warn("Found dirty data.", e);
//                        collector.collectDirtyRecord(record, e.getMessage());
//                    }
//                }

//                insert1(bufferList, collector);

                bufferList.clear();
            }

//            // 类型转换
//            try {
//                RowChange rowChange = getRowChange(record, tableStoreConfig);
//                WithRecord withRecord = (WithRecord) rowChange;
//                withRecord.setRecord(record);
//
//                tableStoreWriter.addRowChange(rowChange);
//
//            } catch (IllegalArgumentException e) {
//                LOG.warn("Found dirty data.", e);
//                collector.collectDirtyRecord(record, e.getMessage());
//            } catch (ClientException e) {
//                LOG.warn("Found dirty data.", e);
//                collector.collectDirtyRecord(record, e.getMessage());
//            }
        }

        if (bufferList.size() > 0) {
            dispatchInsert(bufferList, collector);
            bufferList.clear();
        }

        tableStoreWriter.close();
        LOG.info("Writer slave finished.");
    }

    /**
     * @param record
     * @return
     */
    private RowChange getRowChange(Record record) {
        List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(tableStoreConfig.getAttrColumn(), record);
        PrimaryKey primaryKey = Common.getPKFromRecord(tableStoreConfig, attributes);

        return Common.columnValuesToRowChange(tableStoreConfig.getTableName(), tableStoreConfig.getOperation(), primaryKey, attributes);
    }

    /**
     * 不同写入模式不同导入方式
     *
     * @param bufferList
     * @param collector
     */
    private void dispatchInsert(List<Record> bufferList, TaskPluginCollector collector) {

        String insertMode = tableStoreConfig.getInsertMode();

        if (insertMode.equals("TWM01")) {
            insertTWM01(bufferList, collector);
        } else if (insertMode.equals("TWM02")) {
            insertTWM02(bufferList, collector);
        } else if (insertMode.equals("TWM04")) {
            insertTWM04(bufferList, collector);
        } else if (insertMode.equals("TWM05")) {
            insertTWM05(bufferList, collector);
        }
    }

    /**
     * 构建rowChange 不同写入模式下不同逻辑需构建不同row
     *
     * @param record
     * @param tableStoreOpType 写入|更新|删除
     * @return
     */
    private RowChange getRowChange(Record record, TableStoreOpType tableStoreOpType) {
        List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(tableStoreConfig.getAttrColumn(), record);
        PrimaryKey primaryKey = Common.getPKFromRecord(tableStoreConfig, attributes);

        return Common.columnValuesToRowChange(tableStoreConfig.getTableName(), tableStoreOpType, primaryKey, attributes);
    }

    /**
     * 构建rowChange 不同写入模式下不同逻辑需构建不同row
     *
     * @param record
     * @param tableStoreOpType 写入|更新|删除
     * @return
     */
    private RowChange getExistsRowChange(Record record, TableStoreOpType tableStoreOpType, Row r) {
        List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(tableStoreConfig.getAttrColumn(), record);

        String hashKey = r.getPrimaryKey().getPrimaryKeyColumn(Constant.HASH_KEY).getValue().asString();
        Long increment = r.getPrimaryKey().getPrimaryKeyColumn(Constant.SERIAL_NUMBER).getValue().asLong();

        PrimaryKey primaryKey = Common.getExistRowPk(tableStoreConfig, attributes, hashKey, increment);

        return Common.columnValuesToRowChange(tableStoreConfig.getTableName(), tableStoreOpType, primaryKey, attributes);
    }

    /**
     * 仅插入新条目, 忽略重复条目
     *
     * @param bufferList
     * @param collector
     */
    private void insertTWM01(List<Record> bufferList, TaskPluginCollector collector) {

        Map<String, Row> hashMap = batchGetPrimaryKeyCombo(bufferList);
        List<TableStoreAttrColumn> attrColumn = tableStoreConfig.getAttrColumn();

        for (Record record : bufferList) {

            List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(tableStoreConfig.getAttrColumn(), record);

            String primaryKeyCombo = Common.generatePrimaryKeyCombo(attrColumn, attributes);

            // 忽略已重复行
            if (hashMap.containsKey(primaryKeyCombo)) {
                LOG.info("insertTWM01 ignore row");
                continue;
            }

            // 类型转换
            try {
                RowChange rowChange = getRowChange(record, TableStoreOpType.PUT_ROW);
                WithRecord withRecord = (WithRecord) rowChange;
                withRecord.setRecord(record);

                tableStoreWriter.addRowChange(rowChange);

            } catch (IllegalArgumentException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(record, e.getMessage());
            } catch (ClientException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(record, e.getMessage());
            }
        }

        tableStoreWriter.flush();
    }

    /**
     * 插入新条目, 并更新重复条目
     */
    private void insertTWM02(List<Record> bufferList, TaskPluginCollector collector) {

        Map<String, Row> hashMap = batchGetPrimaryKeyCombo(bufferList);
        List<TableStoreAttrColumn> attrColumn = tableStoreConfig.getAttrColumn();

        List<Record> updateRecords = new ArrayList<Record>();
        List<Record> insertRecords = new ArrayList<Record>();

        for (Record record : bufferList) {

            List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(tableStoreConfig.getAttrColumn(), record);

            String primaryKeyCombo = Common.generatePrimaryKeyCombo(attrColumn, attributes);

            if (hashMap.containsKey(primaryKeyCombo)) {
                updateRecords.add(record);
            } else {
                insertRecords.add(record);
            }
        }

        insertRows(insertRecords, collector);
        updateRows(updateRecords, collector, hashMap);
    }

    /**
     * 清空目标表, 并插入新条
     *
     * @param bufferList
     * @param collector
     */
    private void insertTWM04(List<Record> bufferList, TaskPluginCollector collector) {
        insertRows(bufferList, collector);
    }

    /**
     * 删除重复条目, 插入新条
     *
     * @param bufferList
     * @param collector
     */
    private void insertTWM05(List<Record> bufferList, TaskPluginCollector collector) {

        Map<String, Row> rowMap = batchGetPrimaryKeyCombo(bufferList);
        List<TableStoreAttrColumn> attrColumn = tableStoreConfig.getAttrColumn();

        List<Record> deleteRecords = new ArrayList<Record>();
        List<Record> insertRecords = new ArrayList<Record>();

        for (Record record : bufferList) {

            List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(tableStoreConfig.getAttrColumn(), record);

            String primaryKeyCombo = Common.generatePrimaryKeyCombo(attrColumn, attributes);

            if (rowMap.containsKey(primaryKeyCombo)) {
                deleteRecords.add(record);
            } else {
                insertRecords.add(record);
            }
        }

        deleteRows(deleteRecords, collector, rowMap);
        insertRows(insertRecords, collector);
    }

    /**
     * 记录的主键集合
     * <p>
     * 查询条件为两个字段的组合查询，逻辑表名和对应的主键连接字段
     *
     * @param bufferList
     * @return 存在的主键primary_key_combo字段集合
     */
    private Map<String, Row> batchGetPrimaryKeyCombo(List<Record> bufferList) {

        String tableLogicalName = tableStoreConfig.getTableLogicalName();

        List<TableStoreAttrColumn> attrColumn = tableStoreConfig.getAttrColumn();

        SearchQuery searchQuery = new SearchQuery();
        BoolQuery boolQuery = new BoolQuery();
        TermsQuery primaryKeyComboQuery = new TermsQuery();
        TermQuery tableLogicNameQuery = new TermQuery();

        primaryKeyComboQuery.setFieldName(Constant.PRIMARY_KEY_COMBO);
        for (Record r : bufferList) {
            List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(tableStoreConfig.getAttrColumn(), r);
            String primaryKeyCombo = Common.generatePrimaryKeyCombo(attrColumn, attributes);
            ColumnValue columnValue = new ColumnValue(primaryKeyCombo, ColumnType.STRING);
            primaryKeyComboQuery.addTerm(columnValue);
        }

        tableLogicNameQuery.setFieldName(Constant.TABLE_LOGICAL_NAME);
        tableLogicNameQuery.setTerm(new ColumnValue(tableLogicalName, ColumnType.STRING));

        boolQuery.setMustQueries(Lists.newArrayList(primaryKeyComboQuery, tableLogicNameQuery));

        searchQuery.setQuery(boolQuery);
        searchQuery.setLimit(100); // 最多100个term进行查询，在无重复数据的情况下，最多返回100条数据

        SearchRequest searchRequest = new SearchRequest(tableStoreConfig.getTableName(), Constant.INDEX_NAME, searchQuery);

        SearchResponse search = syncClient.search(searchRequest);

        if (search.getRows() == null || search.getRows().size() == 0) {
            return Collections.emptyMap();
        }

        HashMap<String, Row> hashMap = new HashMap<String, Row>();

        for (Row row : search.getRows()) {
            String primaryKeyCombo = row.getPrimaryKey().getPrimaryKeyColumn(Constant.PRIMARY_KEY_COMBO).getValue().toString();

            hashMap.put(primaryKeyCombo, row);
        }

        return hashMap;
    }

    /**
     * 删除记录
     * 要构建主键
     *
     * @param deleteRecords
     * @param collector
     */
    private void deleteRows(List<Record> deleteRecords, TaskPluginCollector collector, Map<String, Row> rowMap) {

        if (deleteRecords == null || deleteRecords.size() == 0) {
            return;
        }

        List<TableStoreAttrColumn> attrColumn = tableStoreConfig.getAttrColumn();

        for (Record deleteRecord : deleteRecords) {
            // 类型转换
            try {
                List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(tableStoreConfig.getAttrColumn(), deleteRecord);
                String primaryKeyCombo = Common.generatePrimaryKeyCombo(attrColumn, attributes);
                RowChange rowChange = getExistsRowChange(deleteRecord, TableStoreOpType.DELETE_ROW, rowMap.get(primaryKeyCombo));
                WithRecord withRecord = (WithRecord) rowChange;
                withRecord.setRecord(deleteRecord);

                tableStoreWriter.addRowChange(rowChange);

            } catch (IllegalArgumentException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(deleteRecord, e.getMessage());
            } catch (ClientException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(deleteRecord, e.getMessage());
            }
        }

        tableStoreWriter.flush();
    }

    /**
     * 更新记录
     * 要构建主键
     *
     * @param updateRecords
     * @param collector
     */
    private void updateRows(List<Record> updateRecords, TaskPluginCollector collector, Map<String, Row> rowMap) {

        if (updateRecords == null || updateRecords.size() == 0) {
            return;
        }

        List<TableStoreAttrColumn> attrColumn = tableStoreConfig.getAttrColumn();

        for (Record updateRecord : updateRecords) {
            try {
                List<Pair<String, ColumnValue>> attributes = Common.getAttrFromRecord(tableStoreConfig.getAttrColumn(), updateRecord);
                String primaryKeyCombo = Common.generatePrimaryKeyCombo(attrColumn, attributes);
                RowChange rowChange = getExistsRowChange(updateRecord, TableStoreOpType.UPDATE_ROW, rowMap.get(primaryKeyCombo));
                WithRecord withRecord = (WithRecord) rowChange;
                withRecord.setRecord(updateRecord);

                tableStoreWriter.addRowChange(rowChange);

            } catch (IllegalArgumentException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(updateRecord, e.getMessage());
            } catch (ClientException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(updateRecord, e.getMessage());
            }
        }

        tableStoreWriter.flush();
    }

    /**
     * 插入记录
     *
     * @param insertRecords
     * @param collector
     */
    private void insertRows(List<Record> insertRecords, TaskPluginCollector collector) {

        if (insertRecords == null || insertRecords.size() == 0) {
            return;
        }

        for (Record insertRecord : insertRecords) {
            // 类型转换
            try {
                RowChange rowChange = getRowChange(insertRecord, TableStoreOpType.PUT_ROW);
                WithRecord withRecord = (WithRecord) rowChange;
                withRecord.setRecord(insertRecord);

                tableStoreWriter.addRowChange(rowChange);

            } catch (IllegalArgumentException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(insertRecord, e.getMessage());
            } catch (ClientException e) {
                LOG.warn("Found dirty data.", e);
                collector.collectDirtyRecord(insertRecord, e.getMessage());
            }
        }

        tableStoreWriter.flush();
    }
}
