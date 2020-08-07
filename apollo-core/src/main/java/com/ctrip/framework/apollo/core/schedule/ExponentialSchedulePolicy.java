package com.ctrip.framework.apollo.core.schedule;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
// 基于指数退避算法实现的调度策略，指数退避是为了降低对服务端的压力，给服务器一个缓冲恢复的时间
    // 该算法开始是指数退避，当达到delayTimeUpperBound后，不在增加等待时间，而是以delayTimeUpperBound时间固定休眠
public class ExponentialSchedulePolicy implements SchedulePolicy {
  // 计算出的休眠时间，让线程休眠给定的休眠时间，来达到指数退避效果
    // 最小休眠时间
  private final long delayTimeLowerBound;
  // 最大休眠时间
  private final long delayTimeUpperBound;
  // 记录上一次休眠的时间
  private long lastDelayTime;

  public ExponentialSchedulePolicy(long delayTimeLowerBound, long delayTimeUpperBound) {
    this.delayTimeLowerBound = delayTimeLowerBound;
    this.delayTimeUpperBound = delayTimeUpperBound;
  }

  @Override
  public long fail() {
    long delayTime = lastDelayTime;

    if (delayTime == 0) {
        // 初始等待时间，默认是1s
      delayTime = delayTimeLowerBound;
    } else {
        // 将上一次休眠时间*2，然后和delayTimeUpperBound比较，取最小的
      delayTime = Math.min(lastDelayTime << 1, delayTimeUpperBound);
    }

    lastDelayTime = delayTime;

    return delayTime;
  }

  // 成功则重置lastDelayTime为0
  @Override
  public void success() {
    lastDelayTime = 0;
  }
}
