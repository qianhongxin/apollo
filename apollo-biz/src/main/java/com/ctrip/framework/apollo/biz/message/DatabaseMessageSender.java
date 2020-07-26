package com.ctrip.framework.apollo.biz.message;

import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class DatabaseMessageSender implements MessageSender {
  private static final Logger logger = LoggerFactory.getLogger(DatabaseMessageSender.class);
  private static final int CLEAN_QUEUE_MAX_SIZE = 100;
  private BlockingQueue<Long> toClean = Queues.newLinkedBlockingQueue(CLEAN_QUEUE_MAX_SIZE);
  private final ExecutorService cleanExecutorService;
  private final AtomicBoolean cleanStopped;

  private final ReleaseMessageRepository releaseMessageRepository;

  public DatabaseMessageSender(final ReleaseMessageRepository releaseMessageRepository) {
    // 单个线程的线程池，不是定时任务。可以在任务中用死循环处理
    cleanExecutorService = Executors.newSingleThreadExecutor(ApolloThreadFactory.create("DatabaseMessageSender", true));
    cleanStopped = new AtomicBoolean(false);
    this.releaseMessageRepository = releaseMessageRepository;
  }

  @Override
  @Transactional
  public void sendMessage(String message, String channel) {
    logger.info("Sending message {} to channel {}", message, channel);
    if (!Objects.equals(channel, Topics.APOLLO_RELEASE_TOPIC)) {
      logger.warn("Channel {} not supported by DatabaseMessageSender!", channel);
      return;
    }

    Tracer.logEvent("Apollo.AdminService.ReleaseMessage", message);
    // cat调用链监控埋点
    Transaction transaction = Tracer.newTransaction("Apollo.AdminService", "sendMessage");
    try {

      // 保存配置更新保存到数据库
      // 保存的是appId+clusterName+namespace
      ReleaseMessage newMessage = releaseMessageRepository.save(new ReleaseMessage(message));
      // 发布ReleaseMessage的id保存到内存队列
      toClean.offer(newMessage.getId());

      // cat调用链监控埋点
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      logger.error("Sending message to database failed", ex);
      // cat调用链监控埋点
      transaction.setStatus(ex);
      throw ex;
    } finally {
      // cat调用链监控埋点
      transaction.complete();
    }
  }

  // 启动清理releaseMessage的任务
  // 比如同一个Message（即appId+clusterName+namespace），每次更新都会存一条消息到ReleaseMessage表。
  // 比如1+develop+xxx在表中有两条记录： 1，1+develop+xxx   2,1+develop+xxx
  // 这个时候1，1+develop+xxx就要删除掉，保留最新的2，1+develop+xxx即可，因为1+develop+xxx对应的配置就是一个，所以
  // 客户端只要拉取一次即可，所以这里要启动过删除重复消息
  @PostConstruct
  private void initialize() {
    // 死循环处理任务
    cleanExecutorService.submit(() -> {
      while (!cleanStopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          // 取出一个ReleaseMessageId，同时这个id就从队列出队了
          Long rm = toClean.poll(1, TimeUnit.SECONDS);
          if (rm != null) {
            // 清理
            cleanMessage(rm);
          } else {
            TimeUnit.SECONDS.sleep(5);
          }
        } catch (Throwable ex) {
          Tracer.logError(ex);
        }
      }
    });
  }

  private void cleanMessage(Long id) {
    //double check in case the release message is rolled back
    // 查出 id 对应的ReleaseMessage
    ReleaseMessage releaseMessage = releaseMessageRepository.findById(id).orElse(null);
    if (releaseMessage == null) {
      return;
    }
    boolean hasMore = true;
    // 支持中断，循环处理
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      // 根据message（即appId+clusterName+namespace）和Id查出小于id并且message等于给定message的id从小到大前100条记录
      List<ReleaseMessage> messages = releaseMessageRepository.findFirst100ByMessageAndIdLessThanOrderByIdAsc(
          releaseMessage.getMessage(), releaseMessage.getId());

      // 删除这些多余的重复消息。同一个appId+clusterName+namespace，保留一条最新的消息即可
      releaseMessageRepository.deleteAll(messages);
      hasMore = messages.size() == 100;

      messages.forEach(toRemove -> Tracer.logEvent(
          String.format("ReleaseMessage.Clean.%s", toRemove.getMessage()), String.valueOf(toRemove.getId())));
    }
  }

  void stopClean() {
    cleanStopped.set(true);
  }
}
