package com.ctrip.framework.apollo.internals;

import com.ctrip.framework.apollo.enums.ConfigSourceType;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.ClassLoaderUtil;
import com.ctrip.framework.apollo.exceptions.ApolloConfigException;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ConfigUtil;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
// 装饰模式使用，对RemoteConfigRepository做了封装，如果用户不设置m_upstream，就从本地文件取。实现了高可用，降级
public class LocalFileConfigRepository extends AbstractConfigRepository
    implements RepositoryChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(LocalFileConfigRepository.class);
  private static final String CONFIG_DIR = "/config-cache";
  // 存储namespace值
  private final String m_namespace;
  // 文件配置的磁盘文件目录
  private File m_baseDir;
  private final ConfigUtil m_configUtil;
  // 本地文件对应properties
  private volatile Properties m_fileProperties;
  // 关联的远程Repository，即RemoteConfigRepository
  private volatile ConfigRepository m_upstream;

  // 标识配置来源：LOCAL来源本地缓存的文件加载的，ROMOTE：从远程config service抓取的
  private volatile ConfigSourceType m_sourceType = ConfigSourceType.LOCAL;

  /**
   * Constructor.
   *
   * @param namespace the namespace
   */
  public LocalFileConfigRepository(String namespace) {
    this(namespace, null);
  }

  public LocalFileConfigRepository(String namespace, ConfigRepository upstream) {
    m_namespace = namespace;
    m_configUtil = ApolloInjector.getInstance(ConfigUtil.class);
    // 本地磁盘文件的创建，如果不存在
    this.setLocalCacheDir(findLocalCacheDir(), false);
    // 从upstream即RemoteConfigRepository，从远程拉取配置，并放入本地磁盘文件缓存
    this.setUpstreamRepository(upstream);
    // 从本地磁盘文件加载配置缓存到m_fileProperties
    this.trySync();
  }

  void setLocalCacheDir(File baseDir, boolean syncImmediately) {
    m_baseDir = baseDir;
    this.checkLocalConfigCacheDir(m_baseDir);
    if (syncImmediately) {
      this.trySync();
    }
  }

  // 查找本地缓存配置的文件
  private File findLocalCacheDir() {
    try {
        // 本地配置文件路径，linux上是/opt/data/xxx
      String defaultCacheDir = m_configUtil.getDefaultLocalCacheDir();
      Path path = Paths.get(defaultCacheDir);
      if (!Files.exists(path)) {
        Files.createDirectories(path);
      }
      if (Files.exists(path) && Files.isWritable(path)) {
        return new File(defaultCacheDir, CONFIG_DIR);
      }
    } catch (Throwable ex) {
      //ignore
    }

    return new File(ClassLoaderUtil.getClassPath(), CONFIG_DIR);
  }

  @Override
  public Properties getConfig() {
      // 如果m_fileProperties为空，做加载配置操作
    if (m_fileProperties == null) {
      sync();
    }
    // 将配置放在新建的properties中返回
    Properties result = propertiesFactory.getPropertiesInstance();
    result.putAll(m_fileProperties);
    return result;
  }

  @Override
  public void setUpstreamRepository(ConfigRepository upstreamConfigRepository) {
    if (upstreamConfigRepository == null) {
      return;
    }
    //clear previous listener
    if (m_upstream != null) {
        // 从老的m_upstream的监听器列表中移除掉自己。因为119行要更新m_upstream了。也便于gc
      m_upstream.removeChangeListener(this);
    }
    // 更新m_upstream
    m_upstream = upstreamConfigRepository;
    // 从m_upstream指向的Repository即RemoteRepository从远程拉取配置，并合并到LocalFileConfigRepository维护的本地文件中缓存
    trySyncFromUpstream();
    // 监听配置变更，将当前对象LocalFileConfigRepository加入upstreamConfigRepository（即RemoteConfigRepository）的listeners中，当有配置变更时回掉他的onRepositoryChange方法
    upstreamConfigRepository.addChangeListener(this);
  }

  @Override
  public ConfigSourceType getSourceType() {
    return m_sourceType;
  }

  // 配置变更回调，更新本地文件配置
  @Override
  public void onRepositoryChange(String namespace, Properties newProperties) {
      // Properties重写了equals方法。会比较每个key是否完全一样。不是比较内存地址
    if (newProperties.equals(m_fileProperties)) {
        // 如果此次变更的配置和m_fileProperties相同，就不用通知变更了
      return;
    }
    Properties newFileProperties = propertiesFactory.getPropertiesInstance();
    newFileProperties.putAll(newProperties);
    // 变化配置同步到本地文件中
    updateFileProperties(newFileProperties, m_upstream.getSourceType());
      // 通知监听者，配置变更
    this.fireRepositoryChange(namespace, newProperties);
  }

  // 配置同步
  @Override
  protected void sync() {
    //sync with upstream immediately
      // 尝试从远程拉取配置，如果加载成功会同时更新m_fileProperties和本地磁盘文件的配置
    boolean syncFromUpstreamResultSuccess = trySyncFromUpstream();

    if (syncFromUpstreamResultSuccess) {
      // 从远程加载配置成功，这里直接返回
      return;
    }

    // 走到这，说明从远程加载配置失败，直接从本地文件加载
    Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "syncLocalConfig");
    Throwable exception = null;
    try {
      transaction.addData("Basedir", m_baseDir.getAbsolutePath());
      // 将磁盘文件的配置加载到m_fileProperties中
      m_fileProperties = this.loadFromLocalCacheFile(m_baseDir, m_namespace);
      // 设置m_sourceType为LOCAL
      m_sourceType = ConfigSourceType.LOCAL;
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
      transaction.setStatus(ex);
      exception = ex;
      //ignore
    } finally {
      transaction.complete();
    }

    if (m_fileProperties == null) {
      m_sourceType = ConfigSourceType.NONE;
      throw new ApolloConfigException(
          "Load config from local config failed!", exception);
    }
  }

  // 尝试从关联的Repository同步配置
  private boolean trySyncFromUpstream() {
      // 关联的Repository为空，则返回false，同步失败
    if (m_upstream == null) {
      return false;
    }
    try {
        // m_upstream.getConfig()从远程拉取配置，并更新到本地磁盘文件
      updateFileProperties(m_upstream.getConfig(), m_upstream.getSourceType());
      return true;
    } catch (Throwable ex) {
      Tracer.logError(ex);
      logger
          .warn("Sync config from upstream repository {} failed, reason: {}", m_upstream.getClass(),
              ExceptionUtil.getDetailMessage(ex));
    }
    return false;
  }

  private synchronized void updateFileProperties(Properties newProperties, ConfigSourceType sourceType) {
    this.m_sourceType = sourceType;
    // 会比较两个properties中的内容是否一样
    if (newProperties.equals(m_fileProperties)) {
      return;
    }
    // 走到这，说明配置有变化
    this.m_fileProperties = newProperties;
    // 更新本地磁盘文件缓存的配置
    persistLocalCacheFile(m_baseDir, m_namespace);
  }

  // 从本地磁盘文件加载配置并返回
  private Properties loadFromLocalCacheFile(File baseDir, String namespace) throws IOException {
    Preconditions.checkNotNull(baseDir, "Basedir cannot be null");

    // 创建file指向磁盘的配置文件
    File file = assembleLocalCacheFile(baseDir, namespace);
    Properties properties = null;

    if (file.isFile() && file.canRead()) {
      InputStream in = null;

      try {
        in = new FileInputStream(file);
        properties = propertiesFactory.getPropertiesInstance();
        // 读配置
        properties.load(in);
        logger.debug("Loading local config file {} successfully!", file.getAbsolutePath());
      } catch (IOException ex) {
        Tracer.logError(ex);
        throw new ApolloConfigException(String
            .format("Loading config from local cache file %s failed", file.getAbsolutePath()), ex);
      } finally {
        try {
          if (in != null) {
            in.close();
          }
        } catch (IOException ex) {
          // ignore
        }
      }
    } else {
      throw new ApolloConfigException(
          String.format("Cannot read from local cache file %s", file.getAbsolutePath()));
    }

    return properties;
  }

  // 持久化配置到本地磁盘
  void persistLocalCacheFile(File baseDir, String namespace) {
    if (baseDir == null) {
      return;
    }
    // 创建一个新文件
    File file = assembleLocalCacheFile(baseDir, namespace);

    OutputStream out = null;

    Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "persistLocalConfigFile");
    transaction.addData("LocalConfigFile", file.getAbsolutePath());
    try {

      out = new FileOutputStream(file);
      // 覆盖磁盘的旧文件，并将配置更新到m_fileProperties。m_fileProperties内部通过synchronized保证读写并发安全
      m_fileProperties.store(out, "Persisted by DefaultConfig");
      transaction.setStatus(Transaction.SUCCESS);
    } catch (IOException ex) {
      ApolloConfigException exception =
          new ApolloConfigException(
              String.format("Persist local cache file %s failed", file.getAbsolutePath()), ex);
      Tracer.logError(exception);
      transaction.setStatus(exception);
      logger.warn("Persist local cache file {} failed, reason: {}.", file.getAbsolutePath(),
          ExceptionUtil.getDetailMessage(ex));
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException ex) {
          //ignore
        }
      }
      transaction.complete();
    }
  }

  // 检查baseDir是否存在，不存在则创建文件
  private void checkLocalConfigCacheDir(File baseDir) {
    if (baseDir.exists()) {
      return;
    }
    Transaction transaction = Tracer.newTransaction("Apollo.ConfigService", "createLocalConfigDir");
    transaction.addData("BaseDir", baseDir.getAbsolutePath());
    try {
        // 创建文件
      Files.createDirectory(baseDir.toPath());
      transaction.setStatus(Transaction.SUCCESS);
    } catch (IOException ex) {
      ApolloConfigException exception =
          new ApolloConfigException(
              String.format("Create local config directory %s failed", baseDir.getAbsolutePath()),
              ex);
      Tracer.logError(exception);
      transaction.setStatus(exception);
      logger.warn(
          "Unable to create local config cache directory {}, reason: {}. Will not able to cache config file.",
          baseDir.getAbsolutePath(), ExceptionUtil.getDetailMessage(ex));
    } finally {
      transaction.complete();
    }
  }

  File assembleLocalCacheFile(File baseDir, String namespace) {
    String fileName =
        String.format("%s.properties", Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR)
            .join(m_configUtil.getAppId(), m_configUtil.getCluster(), namespace));
    return new File(baseDir, fileName);
  }
}
