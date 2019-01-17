

import com.huawei.common.constants.PathCons;
import com.huawei.security.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.ColumnQualifier;
import org.apache.hadoop.hbase.index.Constants;
import org.apache.hadoop.hbase.index.IndexSpecification;
import org.apache.hadoop.hbase.index.client.IndexAdmin;
import org.apache.hadoop.hbase.index.coprocessor.master.IndexMasterObserver;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;

public class HbaseUtils {
    private static HbaseUtils hbaseUtils;

    private final static Log LOG = LogFactory.getLog(HbaseUtils.class.getName());

    //kerberos用户名
    private static String PRNCIPAL_NAME;

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop";
    private static final String PATH_TO_KEYTAB = PathCons.PATH_TO_KEYTAB;
    private static final String PATH_TO_KRB5_CONF = PathCons.PATH_TO_KRB5_CONF;
    private static final String PATH_TO_HDFS_SITE_XML = PathCons.PATH_TO_HDFS_SITE_XML;
    private static final String PATH_TO_CORE_SITE_XML = PathCons.PATH_TO_CORE_SITE_XML;
    private static final String PATH_TO_HBASE_SITE_XML = PathCons.PATH_TO_HBASE_SITE_XML;
    private static Configuration conf = null;

    private static Connection conn;


    private HbaseUtils() {
    }

    //获取habase单例对象
    public static HbaseUtils getInstance(String keberosUser) {
        PRNCIPAL_NAME = keberosUser;
        //安全校验
        if (hbaseUtils == null) {
            try {
                init();
                login();
                conn = ConnectionFactory.createConnection(conf);
                LOG.info("Login in success");
                return new HbaseUtils();
            } catch (IOException e) {
                LOG.error("Failed to login because ", e);
                System.exit(1);
            }
        }
        return hbaseUtils;
    }

    private static void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_HBASE_SITE_XML));
    }

    private static void login() throws IOException {
        String userdir = System.getProperty("user.dir") + File.separator + "src" + File.separator
                + "main" + File.separator + "resource" + File.separator;
        String userKeytabFile = userdir + "user.keytab";
        if (User.isHBaseSecurityEnabled(conf)) {
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, PRNCIPAL_NAME, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY,
                    ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(PRNCIPAL_NAME, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, conf);
        }

    }

    /**
     * 创建表
     * 列
     *
     * @param tableName  表名
     * @param familyName 列簇名称
     */
    public void createTable(String tableName, String familyName) {
        LOG.info("Entering testCreateTable.");

        HTableDescriptor htd = new HTableDescriptor(tableName);

        HColumnDescriptor hcd = new HColumnDescriptor(familyName);

        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

        hcd.setCompressionType(Compression.Algorithm.SNAPPY);

        htd.addFamily(hcd);

        Admin admin = null;
        try {

            // Instantiate an Admin object.
            admin = conn.getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                LOG.info("Creating table...");
                admin.createTable(htd);
                LOG.info(admin.getClusterStatus());
                LOG.info(admin.listNamespaceDescriptors());
                LOG.info("Table created successfully.");
            } else {
                LOG.warn("table already exists");
            }
        } catch (IOException e) {
            LOG.error("Create table failed.", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Failed to close admin ", e);
                }
            }
        }
        LOG.info("Exiting testCreateTable.");
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public void dropTable(String tableName) {
        LOG.info("Entering dropTable.");

        Admin admin = null;
        try {
            admin = conn.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
//                admin.enableTable(TableName.valueOf(tableName));
                admin.disableTable(TableName.valueOf(tableName));

                // Delete table.
                admin.deleteTable(TableName.valueOf(tableName));
            }
            LOG.info("Drop table successfully.");
        } catch (IOException e) {
            LOG.error("Drop table failed ", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting dropTable.");
    }

    /**
     * 修改表添加列簇
     * HBase通过org.apache.hadoop.hbase.client.Admin的modifyTable方法修改表信息。
     *
     * @param tableName
     * @param family_Name
     */
    public void addFamily(String tableName, String family_Name) {
        LOG.info("Entering ModifyTable.");

        // Specify the column family name.
        byte[] familyName = Bytes.toBytes(family_Name);

        Admin admin = null;
        try {

            admin = conn.getAdmin();

            HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(tableName));

            // 检查是否指定列簇名
            if (!htd.hasFamily(familyName)) {
                // Create the column descriptor.
                HColumnDescriptor hcd = new HColumnDescriptor(familyName);
                htd.addFamily(hcd);

                admin.disableTable((TableName.valueOf(tableName)));

                admin.modifyTable(TableName.valueOf(tableName), htd);

                admin.enableTable(TableName.valueOf(tableName));
            }
            LOG.info("Modify table successfully.");
        } catch (IOException e) {
            LOG.error("Modify table failed ", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting ModifyTable.");
    }

    /**
     * 插入数据
     *
     * @param tableName   表名
     * @param rowkey
     * @param family_Name 列簇
     * @param qualifier   列名
     * @param content     内容
     */
    public void insertData(String tableName, String rowkey, String family_Name, String qualifier, String content) {
        LOG.info("Entering Put Data");

        // 指定列簇名
        byte[] familyName = Bytes.toBytes(family_Name);

        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(familyName, Bytes.toBytes(qualifier), Bytes.toBytes(content));

            table.put(put);

            LOG.info("Put successfully.");
        } catch (IOException e) {
            LOG.error("Put failed ", e);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting Put data");
    }


    /**
     * 根据rowkey删除数据
     *
     * @param tableName 表名
     * @param rowkey
     */
    public void deleteByRowkey(String tableName, String rowkey) {
        LOG.info("Entering deleteByRowkey.");

        byte[] rowKey = Bytes.toBytes(rowkey);

        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Delete delete = new Delete(rowKey);

            table.delete(delete);

            LOG.info("Delete table successfully.");
        } catch (IOException e) {
            LOG.error("Delete table failed ", e);
        } finally {
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting deleteByRowkey");
    }

    /**
     * 根据rowkey获取数据
     *
     * @param tableName 表名
     * @param rowkey
     */
    public void getDataByRowkey(String tableName, String rowkey) {
        LOG.info("Entering GetDataByRowKey");

        byte[] rowKey = Bytes.toBytes(rowkey);

        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            Get get = new Get(rowKey);

            Result result = table.get(get);

            // 打印row
            for (Cell cell : result.rawCells()) {
                LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
                        + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
                        + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                        + Bytes.toString(CellUtil.cloneValue(cell)));
            }

            LOG.info("Get data successfully.");

        } catch (IOException e) {
            LOG.error("Get data failed ", e);
        } finally {
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting GetDataByRowKey.");
    }

    /**
     * 扫描表
     *
     * @param tableName 表名
     */
    public void scanData(String tableName) {
        LOG.info("Entering ScanData.");

        Table table = null;

        ResultScanner rScanner = null;
        try {

            table = conn.getTable(TableName.valueOf(tableName));

            Scan scan = new Scan();
//            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // 设置缓存大小
            scan.setCaching(1000);

            rScanner = table.getScanner(scan);

            // 打印结果
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
                            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Scan data successfully.");
        } catch (IOException e) {
            LOG.error("Scan data failed ", e);
        } finally {
            if (rScanner != null) {
                rScanner.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testScanData.");
    }


    /**
     * 扫描表
     *
     * @param tableName 表名
     */
    public ResultScanner getScanData(String tableName) {
        LOG.info("Entering ScanData.");

        Table table = null;

        ResultScanner rScanner = null;
        try {

            table = conn.getTable(TableName.valueOf(tableName));

            Scan scan = new Scan();
//            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // 设置缓存大小
            scan.setCaching(1000);

            LOG.info("Scan data successfully.");
            return table.getScanner(scan);
            /*ResultScanner scanner = table.getScanner(scan);
            // 打印结果
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
                            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }*/
        } catch (IOException e) {
            LOG.error("Scan data failed ", e);
        } finally {
            if (rScanner != null) {
                rScanner.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        return null;
    }

    /**
     * 设置单列过滤器
     *
     * @param tableName  表名
     * @param familyName 列簇名
     * @param qualifier  列名
     * @param compareOp  比较方式
     * @param value      比较值
     */
    public void singleColumnValueFilter(String tableName, String familyName, String qualifier, CompareFilter.CompareOp compareOp, String value) {
        LOG.info("Entering SingleColumnValueFilter.");

        Table table = null;

        ResultScanner rScanner = null;

        try {
            table = conn.getTable(TableName.valueOf(tableName));

            // 实例化扫描器对象
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));

            // 设置过滤条件
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName),
                    Bytes.toBytes(qualifier), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value));

            scan.setFilter(filter);

            // 提交扫描
            rScanner = table.getScanner(scan);

            // 打印
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
                            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Single column value filter successfully.");
        } catch (IOException e) {
            LOG.error("Single column value filter failed ", e);
        } finally {
            if (rScanner != null) {
                rScanner.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("SingleColumnValueFilter exit");
    }

    /**
     * 创建二级索引
     *
     * @param tableName  表名
     * @param indexName  索引名
     * @param familyName 需要索引的列族
     * @param qualifier  索引列名
     */
    public void createIndex(String tableName, String indexName, String familyName, String qualifier) {
        LOG.info("Entering createIndex.");

        // 创建索引对象
        IndexSpecification iSpec = new IndexSpecification(indexName);

        iSpec.addIndexColumn(new HColumnDescriptor(familyName), qualifier, ColumnQualifier.ValueType.String);

        IndexAdmin iAdmin = null;
        Admin admin = null;
        try {

            iAdmin = new IndexAdmin(conf);

            // 创建二级索引
            iAdmin.addIndex(TableName.valueOf(tableName), iSpec);

            admin = conn.getAdmin();

            // 指定索引列加密方式
            admin.disableTable(TableName.valueOf(tableName));
            HTableDescriptor htd = admin.getTableDescriptor(TableName.valueOf(tableName));

            // 实例化索引列描述
            HColumnDescriptor indexColDesc = new HColumnDescriptor(
                    IndexMasterObserver.DEFAULT_INDEX_COL_DESC);

            htd.setValue(Constants.INDEX_COL_DESC_BYTES, indexColDesc.toByteArray());
            admin.modifyTable(TableName.valueOf(tableName), htd);
            admin.enableTable(TableName.valueOf(tableName));

            LOG.info("Create index successfully.");

        } catch (IOException e) {
            LOG.error("Create index failed.", e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
            if (iAdmin != null) {
                try {
                    iAdmin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting createIndex.");
    }

    /**
     * 删除二级索引
     * @param tableName 表名
     * @param indexName 索引名
     */
    public void dropIndex(String tableName, String indexName) {
        LOG.info("Entering dropIndex.");

        IndexAdmin iAdmin = null;
        try {
            iAdmin = new IndexAdmin(conf);

            iAdmin.dropIndex(TableName.valueOf(tableName), indexName);

            LOG.info("Drop index successfully.");
        } catch (IOException e) {
            LOG.error("Drop index failed ", e);
        } finally {
            if (iAdmin != null) {
                try {
                    iAdmin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting dropIndex.");
    }

    /**
     * 根据二级索引扫描数据
     */
    public void scanDataByIndex(String tableName, String familyName, String qualifier, CompareFilter.CompareOp compareOp, String value) {
        LOG.info("Entering ScanDataByIndex.");

        Table table = null;
        ResultScanner scanner = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));

            // 为索引列创建过滤器
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(familyName), Bytes.toBytes(qualifier),
                    CompareFilter.CompareOp.EQUAL, value.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            scanner = table.getScanner(scan);
            LOG.info("Scan indexed data.");

            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":"
                            + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Scan data by index successfully.");
        } catch (IOException e) {
            LOG.error("Scan data by index failed ", e);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                LOG.error("Close table failed ", e);
            }
        }

        LOG.info("Exiting scanDataByIndex.");
    }
}
