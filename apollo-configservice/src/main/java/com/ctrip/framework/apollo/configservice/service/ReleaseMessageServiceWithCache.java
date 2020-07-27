package com.ctrip.framework.apollo.configservice.service;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.ReleaseMessageListener;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class ReleaseMessageServiceWithCache implements ReleaseMessageListener, InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(ReleaseMessageServiceWithCache
      .class);
  // ReleaseMessage持久层
  private final ReleaseMessageRepository releaseMessageRepository;
  // 配置
  private final BizConfig bizConfig;

  // 扫描间隔
  private int scanInterval;
  private TimeUnit scanIntervalTimeUnit;

  // 最大已扫描id
  private volatile long maxIdScanned;

  // ReleaseMessage缓存
  private ConcurrentMap<String, ReleaseMessage> releaseMessageCache;

  // 扫描开关
  private AtomicBoolean doScan;
  private ExecutorService executorService;

  public ReleaseMessageServiceWithCache(
      final ReleaseMessageRepository releaseMessageRepository,
      final BizConfig bizConfig) {
    this.releaseMessageRepository = releaseMessageRepository;
    this.bizConfig = bizConfig;
    initialize();
  }

  private void initialize() {
    releaseMessageCache = Maps.newConcurrentMap();
    // 扫描开关
    doScan = new AtomicBoolean(true);
    executorService = Executors.newSingleThreadExecutor(ApolloThreadFactory
        .create("ReleaseMessageServiceWithCache", true));
  }

  // 查询messages中最新的ReleaseMessage
  public ReleaseMessage findLatestReleaseMessageForMessages(Set<String> messages) {
    if (CollectionUtils.isEmpty(messages)) {
      return null;
    }

    long maxReleaseMessageId = 0;
    ReleaseMessage result = null;
    for (String message : messages) {
      ReleaseMessage releaseMessage = releaseMessageCache.get(message);
      if (releaseMessage != null && releaseMessage.getId() > maxReleaseMessageId) {
        maxReleaseMessageId = releaseMessage.getId();
        result = releaseMessage;
      }
    }

    return result;
  }

  // 查询 messages 对应的 ReleaseMessage集合，排好序
  public List<ReleaseMessage> findLatestReleaseMessagesGroupByMessages(Set<String> messages) {
    if (CollectionUtils.isEmpty(messages)) {
      return Collections.emptyList();
    }
    List<ReleaseMessage> releaseMessages = Lists.newArrayList();

    for (String message : messages) {
      ReleaseMessage releaseMessage = releaseMessageCache.get(message);
      if (releaseMessage != null) {
        releaseMessages.add(releaseMessage);
      }
    }

    return releaseMessages;
  }

  // 观察者调用的，即ReleaseMessageScanner调用的
  @Override
  public void handleMessage(ReleaseMessage message, String channel) {
    //Could stop once the ReleaseMessageScanner starts to work
    doScan.set(false);
    logger.info("message received - channel: {}, message: {}", channel, message);

    String content = message.getMessage();
    Tracer.logEvent("Apollo.ReleaseMessageService.UpdateCache", String.valueOf(message.getId()));
    if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(content)) {
      return;
    }

    long gap = message.getId() - maxIdScanned;
    if (gap == 1) {
      mergeReleaseMessage(message);
    } else if (gap > 1) {
      //gap found!
      loadReleaseMessages(maxIdScanned);
    }
  }

  // 启动时调用
  @Override
  public void afterPropertiesSet() throws Exception {
    // 初始化扫描时间
    populateDataBaseInterval();
    //block the startup process until load finished
    //this should happen before ReleaseMessageScanner due to autowire
    // 阻塞住spring启动进程，直到加载完成
    loadReleaseMessages(0);

    executorService.submit(() -> {
      // 如果开启扫描，且线程没有打断则执行load
      while (doScan.get() && !Thread.currentThread().isInterrupted()) {
        Transaction transaction = Tracer.newTransaction("Apollo.ReleaseMessageServiceWithCache",
            "scanNewReleaseMessages");
        try {
          loadReleaseMessages(maxIdScanned);
          transaction.setStatus(Transaction.SUCCESS);
        } catch (Throwable ex) {
          transaction.setStatus(ex);
          logger.error("Scan new release messages failed", ex);
        } finally {
          transaction.complete();
        }
        try {
          scanIntervalTimeUnit.sleep(scanInterval);
        } catch (InterruptedException e) {
          //ignore
        }
      }
    });
  }

  private synchronized void mergeReleaseMessage(ReleaseMessage releaseMessage) {
    ReleaseMessage old = releaseMessageCache.get(releaseMessage.getMessage());
    if (old == null || releaseMessage.getId() > old.getId()) {
      releaseMessageCache.put(releaseMessage.getMessage(), releaseMessage);
      maxIdScanned = releaseMessage.getId();
    }
  }

  private void loadReleaseMessages(long startId) {
    boolean hasMore = true;
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      //current batch is 500
      List<ReleaseMessage> releaseMessages = releaseMessageRepository
          .findFirst500ByIdGreaterThanOrderByIdAsc(startId);
      if (CollectionUtils.isEmpty(releaseMessages)) {
        break;
      }
      releaseMessages.forEach(this::mergeReleaseMessage);
      int scanned = releaseMessages.size();
      startId = releaseMessages.get(scanned - 1).getId();
      hasMore = scanned == 500;
      logger.info("Loaded {} release messages with startId {}", scanned, startId);
    }
  }

  private void populateDataBaseInterval() {
    // 设置扫描间隔是1s，每扫描一次睡眠1s
    scanInterval = bizConfig.releaseMessageCacheScanInterval();
    // 扫描单位，秒
    scanIntervalTimeUnit = bizConfig.releaseMessageCacheScanIntervalTimeUnit();
  }

  //only for test use
  private void reset() throws Exception {
    executorService.shutdownNow();
    initialize();
    afterPropertiesSet();
  }
}
