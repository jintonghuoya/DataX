package com.alibaba.datax.plugin.reader.tablestorereader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.tablestorereader.callable.GetTableMetaCallable;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConf;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreConst;
import com.alibaba.datax.plugin.reader.tablestorereader.model.TableStoreRange;
import com.alibaba.datax.plugin.reader.tablestorereader.utils.*;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.SyncClient;
import org.checkerframework.checker.units.qual.K;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableStoreReaderMasterProxy {

    private TableStoreConf conf = new TableStoreConf();

    private TableStoreRange range = null;

    private SyncClient tableStoreClient = null;

    private TableMeta meta = null;

    private Direction direction = null;

    private static final Logger LOG = LoggerFactory.getLogger(TableStoreReaderMasterProxy.class);

    /**
     * 1.检查参数是否为
     * null，endpoint,accessid,accesskey,instance-name,table,column,range-begin,range-end,range-split
     * 2.检查参数是否为空字符串
     * endpoint,accessid,accesskey,instance-name,table
     * 3.检查是否为空数组
     * column
     * 4.检查Range的类型个个数是否和PrimaryKey匹配
     * column,range-begin,range-end
     * 5.检查Range Split 顺序和类型是否Range一致，类型是否于PartitionKey一致
     * column-split
     *
     * @param param
     * @throws Exception
     */
    public void init(Configuration param) throws Exception {
        // 默认参数
        // 每次重试的时间都是上一次的一倍，当sleep时间大于30秒时，Sleep重试时间不在增长。18次能覆盖OTS的Failover时间5分钟
        conf = Common.buildConf(param);

        tableStoreClient = new SyncClient(
                this.conf.getEndpoint(),
                this.conf.getAccessId(),
                this.conf.getAccesskey(),
                this.conf.getInstanceName());

        meta = getTableMeta(tableStoreClient, conf.getTableName());

        LOG.info("Table Meta : {}", GsonParser.metaToJson(meta));
    }

    /**
     * 读取逻辑：
     * 使用token进行深度翻页获取数据，可获取全部数据，但该token不能在进程间共享
     * 故只能提供单进程获取,为提高读取速率，加大读取效率，采用以HashKey分片的形式
     * 读取
     * split 固定分成36个片，即36个任务
     * 配合的查询语句则是，36个任务，均以不同前缀查询，其合集必然是全部结果集，由此提高查询效率
     *
     * @param num
     * @return
     */
    public List<Configuration> split(int num) {
        LOG.info("Expect split num : " + num);

        List<Configuration> configurations = new ArrayList<Configuration>();

        // 因为slave中不会使用这个配置，所以置为空
        this.conf.setRangeSplit(null);

        List<String> hashKeyPrefix = Arrays.asList(
                "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
                "u", "v", "w", "x", "y", "z");

        hashKeyPrefix.forEach(x -> {
            Configuration configuration = Configuration.newDefault();
            configuration.set(TableStoreConst.OTS_CONF, GsonParser.confToJson(this.conf));
            // 执行的最小单位是， 分割后的task进行的，故此设置改config会传递到task中，
            // 起初传递进来的文件不会传递到task中

            configuration.set(TableStoreConst.OTS_DIRECTION, GsonParser.directionToJson(direction));
            configuration.set(TableStoreConst.OTS_HASH_KEY_PREFIX, x);

            configurations.add(configuration);
        });

        LOG.info("Configuration list count : " + configurations.size());

        return configurations;
    }

    public TableStoreConf getConf() {
        return conf;
    }

    public void close() {
        tableStoreClient.shutdown();
    }

    // private function

    private TableMeta getTableMeta(SyncClient ots, String tableName) throws Exception {
        return RetryHelper.executeWithRetry(
                new GetTableMetaCallable(ots, tableName),
                conf.getRetry(),
                conf.getSleepInMilliSecond()
        );
    }
}
