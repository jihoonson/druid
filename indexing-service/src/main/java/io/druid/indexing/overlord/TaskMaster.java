/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.config.TaskQueueConfig;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.DruidNode;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.ThreadUtils;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Encapsulates the indexer leadership lifecycle.
 */
public class TaskMaster
{
  private final LeaderSelector leaderSelector;
  private final ReentrantLock giant = new ReentrantLock(true);
  private final Condition mayBeStopped = giant.newCondition();
  private final TaskActionClientFactory taskActionClientFactory;
  private final SupervisorManager supervisorManager;

  private final AtomicReference<Lifecycle> leaderLifecycleRef = new AtomicReference<>(null);

  private volatile boolean leading = false;
  private volatile TaskRunner taskRunner;
  private volatile TaskQueue taskQueue;

  private static final EmittingLogger log = new EmittingLogger(TaskMaster.class);

  private static class DelayableCloseableExecutorService extends CloseableExecutorService {

    private final int MAX_SLEEP_MS = Integer.MAX_VALUE;
    private final int baseSleepMs = 100;
    private final Random random = new Random();
    private int backoffCount = 0;

    DelayableCloseableExecutorService(ExecutorService executorService)
    {
      super(executorService);
    }

    public<V> Future<V> submit(Callable<V> task)
    {
      sleepForBackoffTime();
      return super.submit(task);
    }

    public Future<?> submit(Runnable task)
    {
      sleepForBackoffTime();
      return super.submit(task);
    }

    private void sleepForBackoffTime()
    {
      final long sleepMs = getSleepTime();
      final Stopwatch stopwatch = Stopwatch.createStarted();

      while (stopwatch.elapsed(TimeUnit.MILLISECONDS) < sleepMs)
      {
        try
        {
          Thread.sleep(sleepMs);
        }
        catch (InterruptedException e)
        {
          log.warn("Interrupted while waiting for backoff time", e);
        }
      }

      stopwatch.stop();
    }

    private long getSleepTime() {
      return Math.min(MAX_SLEEP_MS,
                      baseSleepMs * Math.max(1, random.nextInt(1 << (backoffCount + 1))));
    }

    public void backoff() {
      backoffCount++;
    }
  }

  @Inject
  public TaskMaster(
      final TaskQueueConfig taskQueueConfig,
      final TaskLockbox taskLockbox,
      final TaskStorage taskStorage,
      final TaskActionClientFactory taskActionClientFactory,
      @Self final DruidNode node,
      final IndexerZkConfig zkPaths,
      final TaskRunnerFactory runnerFactory,
      final CuratorFramework curator,
      final ServiceAnnouncer serviceAnnouncer,
      final ServiceEmitter emitter,
      final SupervisorManager supervisorManager
  )
  {
    this.supervisorManager = supervisorManager;
    this.taskActionClientFactory = taskActionClientFactory;

    final DelayableCloseableExecutorService executorService = new DelayableCloseableExecutorService(
        Executors.newSingleThreadExecutor(ThreadUtils.newThreadFactory("LeaderSelector")));

    this.leaderSelector = new LeaderSelector(
        curator,
        zkPaths.getLeaderLatchPath(),
        executorService,
        new LeaderSelectorListener()
        {
          @Override
          public void takeLeadership(CuratorFramework client) throws Exception
          {
            giant.lock();

            try {
              // Make sure the previous leadership cycle is really, really over.
              stopLeading();

              // I AM THE MASTER OF THE UNIVERSE.
              log.info("By the power of Grayskull, I have the power!");
              taskLockbox.syncFromStorage();
              taskRunner = runnerFactory.build();
              taskQueue = new TaskQueue(
                  taskQueueConfig,
                  taskStorage,
                  taskRunner,
                  taskActionClientFactory,
                  taskLockbox,
                  emitter
              );

              // Sensible order to start stuff:
              final Lifecycle leaderLifecycle = new Lifecycle();
              if (leaderLifecycleRef.getAndSet(leaderLifecycle) != null) {
                log.makeAlert("TaskMaster set a new Lifecycle without the old one being cleared!  Race condition")
                   .emit();
              }

              leaderLifecycle.addManagedInstance(taskRunner);
              leaderLifecycle.addManagedInstance(taskQueue);
              leaderLifecycle.addManagedInstance(supervisorManager);

              leaderLifecycle.addHandler(
                  new Lifecycle.Handler()
                  {
                    @Override
                    public void start() throws Exception
                    {
                      serviceAnnouncer.announce(node);
                    }

                    @Override
                    public void stop()
                    {
                      serviceAnnouncer.unannounce(node);
                    }
                  }
              );
              try {
                leaderLifecycle.start();
                leading = true;
                while (leading && !Thread.currentThread().isInterrupted()) {
                  mayBeStopped.await();
                }
              }
              catch (InterruptedException e) {
                log.debug("Interrupted while waiting");
                // Suppress so we can bow out gracefully
              }
              finally {
                log.info("Bowing out!");
                stopLeading();
              }
            }
            catch (Exception e) {
              log.makeAlert(e, "Failed to lead").emit();
              throw Throwables.propagate(e);
            }
            finally {
              giant.unlock();
            }
          }

          @Override
          public void stateChanged(CuratorFramework client, ConnectionState newState)
          {
            if (newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
              // disconnected from zk. assume leadership is gone
              stopLeading();
              executorService.backoff();
            }
          }
        }
    );

    leaderSelector.setId(node.getHostAndPort());
    leaderSelector.autoRequeue();
  }

  /**
   * Starts waiting for leadership. Should only be called once throughout the life of the program.
   */
  @LifecycleStart
  public void start()
  {
    giant.lock();

    try {
      leaderSelector.start();
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Stops forever (not just this particular leadership session). Should only be called once throughout the life of
   * the program.
   */
  @LifecycleStop
  public void stop()
  {
    giant.lock();

    try {
      leaderSelector.close();
      stopLeading();
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Relinquish leadership. May be called multiple times, even when not currently the leader.
   */
  private void stopLeading()
  {
    giant.lock();

    try {
      if (leading) {
        leading = false;
        mayBeStopped.signalAll();
        final Lifecycle leaderLifecycle = leaderLifecycleRef.getAndSet(null);
        if (leaderLifecycle != null) {
          leaderLifecycle.stop();
        }
      }
    }
    finally {
      giant.unlock();
    }
  }

  public boolean isLeading()
  {
    return leading;
  }

  public String getLeader()
  {
    try {
      final Participant leader = leaderSelector.getLeader();
      if (leader != null && leader.isLeader()) {
        return leader.getId();
      } else {
        return null;
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Optional<TaskRunner> getTaskRunner()
  {
    if (leading) {
      return Optional.of(taskRunner);
    } else {
      return Optional.absent();
    }
  }

  public Optional<TaskQueue> getTaskQueue()
  {
    if (leading) {
      return Optional.of(taskQueue);
    } else {
      return Optional.absent();
    }
  }

  public Optional<TaskActionClient> getTaskActionClient(Task task)
  {
    if (leading) {
      return Optional.of(taskActionClientFactory.create(task));
    } else {
      return Optional.absent();
    }
  }

  public Optional<ScalingStats> getScalingStats()
  {
    if (leading) {
      return taskRunner.getScalingStats();
    } else {
      return Optional.absent();
    }
  }

  public Optional<SupervisorManager> getSupervisorManager()
  {
    if (leading) {
      return Optional.of(supervisorManager);
    } else {
      return Optional.absent();
    }
  }
}
