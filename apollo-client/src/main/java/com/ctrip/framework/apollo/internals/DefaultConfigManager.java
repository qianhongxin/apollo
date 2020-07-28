package com.ctrip.framework.apollo.internals;

import java.util.Map;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.spi.ConfigFactory;
import com.ctrip.framework.apollo.spi.ConfigFactoryManager;
import com.google.common.collect.Maps;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
// DefaultConfigManager管理各个namespace的Config和Configfile对象，是门面模式使用，封装复杂度
public class DefaultConfigManager implements ConfigManager {
    // 获取创建Config的工厂
  private ConfigFactoryManager m_factoryManager;
  // 存储namespace和Config的对应关系
  private Map<String, Config> m_configs = Maps.newConcurrentMap();
    // 存储namespace和 ConfigFile 的对应关系
  private Map<String, ConfigFile> m_configFiles = Maps.newConcurrentMap();

  public DefaultConfigManager() {
      // 创建ConfigFactoryManager，单例对象
    m_factoryManager = ApolloInjector.getInstance(ConfigFactoryManager.class);
  }

  @Override
  public Config getConfig(String namespace) {
      // 获取namespace对应的Config对象
    Config config = m_configs.get(namespace);

    if (config == null) {
        // 给DefaultConfigManager单例对象加锁
      synchronized (this) {
        config = m_configs.get(namespace);

        if (config == null) {
          ConfigFactory factory = m_factoryManager.getFactory(namespace);

          config = factory.create(namespace);
          m_configs.put(namespace, config);
        }
      }
    }

    return config;
  }

  @Override
  public ConfigFile getConfigFile(String namespace, ConfigFileFormat configFileFormat) {
    String namespaceFileName = String.format("%s.%s", namespace, configFileFormat.getValue());
    ConfigFile configFile = m_configFiles.get(namespaceFileName);

    if (configFile == null) {
      synchronized (this) {
        configFile = m_configFiles.get(namespaceFileName);

        if (configFile == null) {
          ConfigFactory factory = m_factoryManager.getFactory(namespaceFileName);

          configFile = factory.createConfigFile(namespaceFileName, configFileFormat);
          m_configFiles.put(namespaceFileName, configFile);
        }
      }
    }

    return configFile;
  }
}
