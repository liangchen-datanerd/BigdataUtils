
import com.huawei.common.constants.PathCons;
import com.huawei.common.exception.ParameterException;
import com.huawei.security.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;


public class HdfsUtils {

    private static HdfsUtils hdfsUtils;

    private final static Log LOG = LogFactory.getLog(HdfsUtils.class.getName());

    private static FileSystem fSystem; /* HDFS file system */
    private static Configuration conf;

    private static String PRNCIPAL_NAME;
    private static String PATH_TO_KEYTAB = PathCons.PATH_TO_KEYTAB;
    private static String PATH_TO_KRB5_CONF = PathCons.PATH_TO_KRB5_CONF;
    private static String PATH_TO_HDFS_SITE_XML = PathCons.PATH_TO_HDFS_SITE_XML;
    private static String PATH_TO_CORE_SITE_XML = PathCons.PATH_TO_CORE_SITE_XML;

    private HdfsUtils() {
    }

    /**
     * 获取HDFS单例对象
     * @param keberosUser keberos认证用户名
     * @return
     */
    public static HdfsUtils getInstance(String keberosUser) throws IOException {
        PRNCIPAL_NAME = keberosUser;
        if (hdfsUtils == null) {
            init();
            return new HdfsUtils();
        }
        return hdfsUtils;
    }

    /**
     * 加载conf文件
     * 认证kerberos
     * 获取FileSystem实例
     */
    private static void init() throws IOException {
        confLoad();
        authentication();
        instanceBuild();
    }

    /**
     * 加载配置文件如core-site.xml，hdfs-site.xml
     */
    private static void confLoad() throws IOException {
        conf = new Configuration();
        // conf file
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));
    }

    /**
     * kerberos认证
     */
    private static void authentication() throws IOException {
        // security mode
        LOG.info(conf.get("hadoop.security.authentication"));
        if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication"))) {
            System.setProperty("java.security.krb5.conf", PATH_TO_KRB5_CONF);
            LoginUtil.login(PRNCIPAL_NAME, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, conf);
        }
    }

    /**
     * 构建FileSystem实例
     */
    private static void instanceBuild() throws IOException {
        // get filesystem
        fSystem = FileSystem.get(conf);
    }

    /**
     * 在HDFS上创建目录
     * @param filePath
     * @throws IOException
     */
    public void mkdir(String filePath) throws IOException {
        Path destPath = new Path(filePath);
        if (fSystem.exists(destPath)) {
            LOG.error("failed to create destPath the path is already existed: " + filePath);
        } else {
            fSystem.mkdirs(destPath);
            LOG.info("success to create destPath " + filePath);
        }
    }

    /**
     * 往HDFS上写数据
     * @param content 内容
     * @param destPath 目标目录
     * @param fileName 文件名(eg test.txt)
     * @throws IOException
     */
    public void write(final String content, String destPath, String fileName) throws IOException, ParameterException {
        InputStream in = (InputStream) new ByteArrayInputStream(content.getBytes());
        try {
            HdfsWriter writer = new HdfsWriter(fSystem, destPath + File.separator + fileName);
            writer.doWrite(in);
            LOG.info("success to write.");
        } finally {
            in.close();
        }
    }

    /**
     * 往指定目录下指定文件追加内容
     * @param content 追加内容
     * @param destPath 目标目录
     * @param fileName 文件名(eg test.txt)
     * @throws Exception
     */
    public void append(final String content, String destPath, String fileName) throws Exception {
        //校验路径是否存在
        if (!fSystem.exists(new Path(destPath+"/"+fileName))) {
            LOG.error("the Path or File doesn't exists " + destPath+"/"+fileName);
        } else {
            InputStream in = (InputStream) new ByteArrayInputStream(content.getBytes());
            try {
                HdfsWriter writer = new HdfsWriter(fSystem, destPath + File.separator + fileName);
                writer.doAppend(in);
                LOG.info("success to append.");
            } finally {
                close(in);
            }
        }
    }

    /**
     * 读取HDFS文件
     * @param destPath HDFS目录
     * @param fileName 文件名(eg test.txt)
     * @return
     * @throws IOException
     */
    public String read(String destPath, String fileName) throws IOException {
        if (!fSystem.exists(new Path(destPath+"/"+fileName))) {
            LOG.error("the Path or File doesn't exists " + destPath+"/"+fileName);
        } else {
            String strPath = destPath + File.separator + fileName;
            Path path = new Path(strPath);
            FSDataInputStream in = null;
            BufferedReader reader = null;
            StringBuffer strBuffer = new StringBuffer();

            try {
                in = fSystem.open(path);
                reader = new BufferedReader(new InputStreamReader(in));
                String sTempOneLine;

                while ((sTempOneLine = reader.readLine()) != null) {
                    strBuffer.append(sTempOneLine + "\n");
                }

                LOG.info("success to read ");
                return strBuffer.toString();

            } finally {
                close(reader);
                close(in);
            }
        }
        return null;
    }

    /**
     * 删除文件
     * @param destPath 目标目录
     * @param fileName 文件名
     * @throws IOException
     */
    public void deleteFile(String destPath, String fileName) throws IOException {
        Path beDeletedPath = new Path(destPath + File.separator + fileName);
        if (fSystem.delete(beDeletedPath, true)) {
            LOG.info("success to delete the file " + destPath + File.separator + fileName);
        } else {
            LOG.error("failed to delete the file, the file doesn't exists" + destPath + File.separator + fileName);
        }
    }

    /**
     * 删除HDFS目录及目录下文件
     * @param destPath 目标目录
     * @throws IOException
     */
    public void rmdir(String destPath) throws IOException {
        Path path = new Path(destPath);
        if (fSystem.exists(path)) {
            fSystem.delete(path, true);
            LOG.info("success to delete path " + destPath);
        } else {
            LOG.error("failed to delete destPath, the directory doesn't exists " + destPath);
        }
    }

    /**
     * 上传本地单个本地文件或文件夹到HDFS
     * @param localPath 本地文件或者文件目录
     * @param destPath HDFS中的目录，或者文件名
     */
    public void putFile(String localPath, String destPath) {
        try {
            // 从本地将文件拷贝到HDFS中，如果目标文件已存在则进行覆盖
            fSystem.copyFromLocalFile(new Path(localPath), new Path(destPath));
            LOG.info("file has been uploaded to HDFS");
        } catch (IOException e) {
            LOG.error("uploading failed");
            e.printStackTrace();
        }
    }

    /**
     * 下载HDFS单个本地文件或文件夹到本地
     *
     * @param destPath HDFS中文件或目录
     * @param localPath 本地目录
     */
    public void getFile(String destPath, String localPath) {
        try {
            if (!fSystem.exists(new Path(destPath))) {
                LOG.error("文件不存在！");
            } else {
                fSystem.copyToLocalFile(new Path(destPath), new Path(localPath));
                LOG.info("download succeed");
            }
        } catch (IOException e) {
            LOG.error("downloading failed");
            e.printStackTrace();
        }
    }


    //关闭流
    private void close(Closeable stream) throws IOException {
        stream.close();
    }
}
