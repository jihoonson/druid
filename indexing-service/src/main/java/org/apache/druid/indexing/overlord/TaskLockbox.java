/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.overlord;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Remembers which activeTasks have locked which intervals. Tasks are permitted to lock an interval if no other task
 * outside their group has locked an overlapping interval for the same datasource. When a task locks an interval,
 * it is assigned a version string that it can use to publish segments.
 */
public class TaskLockbox
{
  // Datasource -> Interval -> list of (Tasks + TaskLock)
  // Multiple shared locks can be acquired for the same dataSource and interval.
  // Note that revoked locks are also maintained in this map to notify that those locks are revoked to the callers when
  // they acquire the same locks again.
  private final Map<String, NavigableMap<Interval, List<TaskLockPosse>>> running = new HashMap<>();
  private final TaskStorage taskStorage;
  private final ReentrantLock giant = new ReentrantLock(true);
  private final Condition lockReleaseCondition = giant.newCondition();

  private static final EmittingLogger log = new EmittingLogger(TaskLockbox.class);

  // Stores List of Active Tasks. TaskLockbox will only grant locks to active activeTasks.
  // this set should be accessed under the giant lock.
  private final Set<String> activeTasks = new HashSet<>();

  @Inject
  public TaskLockbox(
      TaskStorage taskStorage
  )
  {
    this.taskStorage = taskStorage;
  }

  /**
   * Wipe out our current in-memory state and resync it from our bundled {@link TaskStorage}.
   */
  public void syncFromStorage()
  {
    giant.lock();

    try {
      // Load stuff from taskStorage first. If this fails, we don't want to lose all our locks.
      final Set<String> storedActiveTasks = new HashSet<>();
      final List<Pair<Task, TaskLock>> storedLocks = new ArrayList<>();
      for (final Task task : taskStorage.getActiveTasks()) {
        storedActiveTasks.add(task.getId());
        for (final TaskLock taskLock : taskStorage.getLocks(task.getId())) {
          storedLocks.add(Pair.of(task, taskLock));
        }
      }
      // Sort locks by version, so we add them back in the order they were acquired.
      final Ordering<Pair<Task, TaskLock>> byVersionOrdering = new Ordering<Pair<Task, TaskLock>>()
      {
        @Override
        public int compare(Pair<Task, TaskLock> left, Pair<Task, TaskLock> right)
        {
          // The second compare shouldn't be necessary, but, whatever.
          return ComparisonChain.start()
                                .compare(left.rhs.getVersion(), right.rhs.getVersion())
                                .compare(left.lhs.getId(), right.lhs.getId())
                                .result();
        }
      };
      running.clear();
      activeTasks.clear();
      activeTasks.addAll(storedActiveTasks);
      // Bookkeeping for a log message at the end
      int taskLockCount = 0;
      for (final Pair<Task, TaskLock> taskAndLock : byVersionOrdering.sortedCopy(storedLocks)) {
        final Task task = Preconditions.checkNotNull(taskAndLock.lhs, "task");
        final TaskLock savedTaskLock = Preconditions.checkNotNull(taskAndLock.rhs, "savedTaskLock");
        if (savedTaskLock.getInterval().toDurationMillis() <= 0) {
          // "Impossible", but you never know what crazy stuff can be restored from storage.
          log.warn("WTF?! Got lock[%s] with empty interval for task: %s", savedTaskLock, task.getId());
          continue;
        }

        // Create a new taskLock if it doesn't have a proper priority,
        // so that every taskLock in memory has the priority.
        final TaskLock savedTaskLockWithPriority = savedTaskLock.getPriority() == null
                                      ? savedTaskLock.withPriority(task.getPriority())
                                      : savedTaskLock;

        final TaskLockPosse taskLockPosse = verifyAndCreateOrFindLockPosse(task, savedTaskLockWithPriority);
        if (taskLockPosse != null) {
          taskLockPosse.addTask(task);

          final TaskLock taskLock = taskLockPosse.getTaskLock();

          if (savedTaskLockWithPriority.getVersion().equals(taskLock.getVersion())) {
            taskLockCount++;
            log.info(
                "Reacquired lock[%s] for task: %s",
                taskLock,
                task.getId()
            );
          } else {
            taskLockCount++;
            log.info(
                "Could not reacquire lock on interval[%s] version[%s] (got version[%s] instead) for task: %s",
                savedTaskLockWithPriority.getInterval(),
                savedTaskLockWithPriority.getVersion(),
                taskLock.getVersion(),
                task.getId()
            );
          }
        } else {
          throw new ISE(
              "Could not reacquire lock on interval[%s] version[%s] for task: %s",
              savedTaskLockWithPriority.getInterval(),
              savedTaskLockWithPriority.getVersion(),
              task.getId()
          );
        }
      }
      log.info(
          "Synced %,d locks for %,d activeTasks from storage (%,d locks ignored).",
          taskLockCount,
          activeTasks.size(),
          storedLocks.size() - taskLockCount
      );
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * This method is called only in {@link #syncFromStorage()} and verifies the given task and the taskLock have the same
   * groupId, dataSource, and priority.
   */
  @Nullable
  private TaskLockPosse verifyAndCreateOrFindLockPosse(Task task, TaskLock taskLock)
  {
    giant.lock();

    try {
      Preconditions.checkArgument(
          task.getGroupId().equals(taskLock.getGroupId()),
          "lock groupId[%s] is different from task groupId[%s]",
          taskLock.getGroupId(),
          task.getGroupId()
      );
      Preconditions.checkArgument(
          task.getDataSource().equals(taskLock.getDataSource()),
          "lock dataSource[%s] is different from task dataSource[%s]",
          taskLock.getDataSource(),
          task.getDataSource()
      );
      final int taskPriority = task.getPriority();
      final int lockPriority = taskLock.getNonNullPriority();

      Preconditions.checkArgument(
          lockPriority == taskPriority,
          "lock priority[%s] is different from task priority[%s]",
          lockPriority,
          taskPriority
      );

      final LockRequest request;
      switch (taskLock.getGranularity()) {
        case SEGMENT:
          final SegmentLock segmentLock = (SegmentLock) taskLock;
          request = new SegmentLockRequest(
              segmentLock.getType(),
              segmentLock.getGroupId(),
              segmentLock.getDataSource(),
              segmentLock.getInterval(),
              segmentLock.getPartitionIds(),
              segmentLock.getVersion(),
              taskPriority,
              segmentLock.isRevoked()
          );
          break;
        case TIME_CHUNK:
          final TimeChunkLock timeChunkLock = (TimeChunkLock) taskLock;
          request = new TimeChunkLockRequest(
              timeChunkLock.getType(),
              timeChunkLock.getGroupId(),
              timeChunkLock.getDataSource(),
              timeChunkLock.getInterval(),
              timeChunkLock.getVersion(),
              taskPriority,
              timeChunkLock.isRevoked()
          );
          break;
        default:
          throw new ISE("Unknown lockGranularity[%s]", taskLock.getGranularity());
      }

      return createOrFindLockPosse(task, request);
    }
    finally {
      giant.unlock();
    }
  }

  public LockResult lock(
      final TaskLockType lockType,
      final Task task,
      final Interval interval
  ) throws InterruptedException
  {
    return lock(LockGranularity.TIME_CHUNK, lockType, task, interval, null, null);
  }

  /**
   * Acquires a lock on behalf of a task.  Blocks until the lock is acquired.
   *
   * @param granularity lock granularity
   * @param lockType lock type
   * @param task     task to acquire lock for
   * @param interval interval to lock
   *
   * @return {@link LockResult} containing a new or an existing lock if succeeded. Otherwise, {@link LockResult} with a
   * {@link LockResult#revoked} flag.
   *
   * @throws InterruptedException if the current thread is interrupted
   */
  public LockResult lock(
      final LockGranularity granularity,
      final TaskLockType lockType,
      final Task task,
      final Interval interval,
      final String version,
      final Set<Integer> partitionIds
  ) throws InterruptedException
  {
    giant.lockInterruptibly();
    try {
      LockResult lockResult;
      while (!(lockResult = tryLock(granularity, lockType, task, interval, version, partitionIds)).isOk()) {
        if (lockResult.isRevoked()) {
          return lockResult;
        }
        lockReleaseCondition.await();
      }
      return lockResult;
    }
    finally {
      giant.unlock();
    }
  }

  public LockResult lock(
      final TaskLockType lockType,
      final Task task,
      final Interval interval,
      long timeoutMs
  ) throws InterruptedException
  {
    return lock(LockGranularity.TIME_CHUNK, lockType, task, interval, null, null, timeoutMs);
  }

  /**
   * Acquires a lock on behalf of a task, waiting up to the specified wait time if necessary.
   *
   * @param granularity lock granularity
   * @param lockType  lock type
   * @param task      task to acquire a lock for
   * @param interval  interval to lock
   * @param timeoutMs maximum time to wait
   *
   * @return {@link LockResult} containing a new or an existing lock if succeeded. Otherwise, {@link LockResult} with a
   * {@link LockResult#revoked} flag.
   *
   * @throws InterruptedException if the current thread is interrupted
   */
  public LockResult lock(
      final LockGranularity granularity,
      final TaskLockType lockType,
      final Task task,
      final Interval interval,
      final String version,
      final Set<Integer> partitionIds,
      long timeoutMs
  ) throws InterruptedException
  {
    long nanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    giant.lockInterruptibly();
    try {
      LockResult lockResult;
      while (!(lockResult = tryLock(granularity, lockType, task, interval, version, partitionIds)).isOk()) {
        if (nanos <= 0 || lockResult.isRevoked()) {
          return lockResult;
        }
        nanos = lockReleaseCondition.awaitNanos(nanos);
      }
      return lockResult;
    }
    finally {
      giant.unlock();
    }
  }

  private static LockRequest createRequest(
      final LockGranularity granularity,
      final TaskLockType lockType,
      final Task task,
      final Interval interval,
      final String version,
      final Set<Integer> partitionIds
  )
  {
    switch (granularity) {
      case TIME_CHUNK:
        return new TimeChunkLockRequest(
            lockType,
            task.getGroupId(),
            task.getDataSource(),
            interval,
            version,
            task.getPriority(),
            false
        );
      case SEGMENT:
        return new SegmentLockRequest(
            lockType,
            task.getGroupId(),
            task.getDataSource(),
            interval,
            partitionIds,
            version,
            task.getPriority(),
            false
        );
      default:
        throw new ISE("Unknown lockGranularity[%s]", granularity);
    }
  }

  @VisibleForTesting
  public LockResult tryTimeChunkLock(
      final TaskLockType lockType,
      final Task task,
      final Interval interval
  )
  {
    return tryLock(LockGranularity.TIME_CHUNK, lockType, task, interval, null, null);
  }

  /**
   * Attempt to acquire a lock for a task, without removing it from the queue. Can safely be called multiple times on
   * the same task until the lock is preempted.
   *
   * @param granularity lock granularity
   * @param lockType type of lock to be acquired
   * @param task     task that wants a lock
   * @param interval interval to lock
   *
   * @return {@link LockResult} containing a new or an existing lock if succeeded. Otherwise, {@link LockResult} with a
   * {@link LockResult#revoked} flag.
   *
   * @throws IllegalStateException if the task is not a valid active task
   */
  public LockResult tryLock(
      final LockGranularity granularity,
      final TaskLockType lockType,
      final Task task,
      final Interval interval,
      final String version,
      final Set<Integer> partitionIds
  )
  {
    giant.lock();

    try {
      if (!activeTasks.contains(task.getId())) {
        throw new ISE("Unable to grant lock to inactive Task [%s]", task.getId());
      }
      Preconditions.checkArgument(interval.toDurationMillis() > 0, "interval empty");

      final LockRequest request = createRequest(granularity, lockType, task, interval, version, partitionIds);
      final TaskLockPosse posseToUse = createOrFindLockPosse(task, request);
      if (posseToUse != null && !posseToUse.getTaskLock().isRevoked()) {
        // Add to existing TaskLockPosse, if necessary
        if (posseToUse.addTask(task)) {
          log.info("Added task[%s] to TaskLock[%s]", task.getId(), posseToUse.getTaskLock().getGroupId());

          // Update task storage facility. If it fails, revoke the lock.
          try {
            taskStorage.addLock(task.getId(), posseToUse.getTaskLock());
            return LockResult.ok(posseToUse.getTaskLock());
          }
          catch (Exception e) {
            log.makeAlert("Failed to persist lock in storage")
               .addData("task", task.getId())
               .addData("dataSource", posseToUse.getTaskLock().getDataSource())
               .addData("interval", posseToUse.getTaskLock().getInterval())
               .addData("version", posseToUse.getTaskLock().getVersion())
               .emit();
            unlock(task, interval);
            return LockResult.fail(false);
          }
        } else {
          log.info("Task[%s] already present in TaskLock[%s]", task.getId(), posseToUse.getTaskLock().getGroupId());
          return LockResult.ok(posseToUse.getTaskLock());
        }
      } else {
        final boolean lockRevoked = posseToUse != null && posseToUse.getTaskLock().isRevoked();
        return LockResult.fail(lockRevoked);
      }
    }
    finally {
      giant.unlock();
    }
  }

  @Nullable
  private TaskLockPosse createOrFindLockPosse(Task task, LockRequest request)
  {
    giant.lock();

    try {
      final String taskId = task.getId();
      final List<TaskLockPosse> foundPosses = findLockPossesOverlapsInterval(
          request.getDataSource(),
          request.getInterval()
      );

      final List<TaskLockPosse> conflictPosses = foundPosses
          .stream()
          .filter(taskLockPosse -> taskLockPosse.getTaskLock().conflict(request))
          .collect(Collectors.toList());

      if (conflictPosses.size() > 0) {
        // If we have some locks for dataSource and interval, check they can be reused.
        // If they can't be reused, check lock priority and revoke existing locks if possible.
        final List<TaskLockPosse> reusablePosses = conflictPosses
            .stream()
            .filter(posse -> posse.reusableFor(task, request))
            .collect(Collectors.toList());

        if (reusablePosses.size() == 0) {
          // case 1) this task doesn't have any lock, but others do

          if (request.getLockType().equals(TaskLockType.SHARED) && isAllSharedLocks(conflictPosses)) {
            // Any number of shared locks can be acquired for the same dataSource and interval.
            return createNewTaskLockPosse(request);
          } else {
            if (isAllRevocable(conflictPosses, request.getPriority())) {
              // Revoke all existing locks
              conflictPosses.forEach(this::revokeLock);

              return createNewTaskLockPosse(request);
            } else {
              final String messagePrefix;
              if (request.getPreferredVersion() == null) {
                messagePrefix = StringUtils.format(
                    "Cannot create a new taskLockPosse for task[%s], interval[%s], priority[%d], revoked[%s]",
                    taskId,
                    request.getInterval(),
                    request.getPriority(),
                    request.isRevoked()
                );
              } else {
                messagePrefix = StringUtils.format(
                    "Cannot create a new taskLockPosse for task[%s], interval[%s],"
                    + " preferredVersion[%s], priority[%d], revoked[%s]",
                    taskId,
                    request.getInterval(),
                    request.getPreferredVersion(),
                    request.getPriority(),
                    request.isRevoked()
                );
              }

              log.info(
                  "%s because existing locks[%s] have same or higher priorities",
                  messagePrefix,
                  conflictPosses
              );
              return null;
            }
          }
        } else if (reusablePosses.size() == 1) {
          // case 2) we found a lock posse for the given task
          final TaskLockPosse foundPosse = reusablePosses.get(0);
          if (request.getLockType().equals(foundPosse.getTaskLock().getType()) &&
              request.getGranularity() == foundPosse.getTaskLock().getGranularity()) {
            return foundPosse;
          } else {
            if (request.getLockType() != foundPosse.getTaskLock().getType()) {
              throw new ISE(
                  "Task[%s] already acquired a lock for interval[%s] but different type[%s]",
                  taskId,
                  request.getInterval(),
                  foundPosse.getTaskLock().getType()
              );
            } else {
              throw new ISE(
                  "Task[%s] already acquired a lock for interval[%s] but different granularity[%s]",
                  taskId,
                  request.getInterval(),
                  foundPosse.getTaskLock().getGranularity()
              );
            }
          }
        } else {
          // case 3) we found multiple lock posses for the given task
          throw new ISE(
              "Task group[%s] has multiple locks for the same interval[%s]?",
              request.getGroupId(),
              request.getInterval()
          );
        }
      } else {
        // We don't have any locks for dataSource and interval.
        // Let's make a new one.
        return createNewTaskLockPosse(request);
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Create a new {@link TaskLockPosse} for a new {@link TaskLock}. This method will attempt to assign version strings
   * that obey the invariant that every version string is lexicographically greater than any other version string
   * previously assigned to the same interval. This invariant is only mostly guaranteed, however; we assume clock
   * monotonicity and that callers specifying {@code preferredVersion} are doing the right thing.
   *
   * @param request request to lock
   *
   * @return a new {@link TaskLockPosse}
   */
  private TaskLockPosse createNewTaskLockPosse(LockRequest request)
  {
    giant.lock();
    try {
      final TaskLockPosse posseToUse = new TaskLockPosse(request.toLock());
      running.computeIfAbsent(request.getDataSource(), k -> new TreeMap<>(Comparators.intervalsByStartThenEnd()))
             .computeIfAbsent(request.getInterval(), k -> new ArrayList<>())
             .add(posseToUse);

      return posseToUse;
    }
    finally {
      giant.unlock();
    }
  }

  public interface LockRequest
  {
    LockGranularity getGranularity();

    TaskLockType getLockType();

    String getGroupId();

    String getDataSource();

    Interval getInterval();

    @Nullable
    String getPreferredVersion();

    int getPriority();

    boolean isRevoked();

    TaskLock toLock();
  }

  public static class TimeChunkLockRequest implements LockRequest
  {
    private final TaskLockType lockType;
    private final String groupId;
    private final String dataSource;
    private final Interval interval;
    @Nullable private final String preferredVersion;
    private final int priority;
    private final boolean revoked;

    private TimeChunkLockRequest(
        TaskLockType lockType,
        String groupId,
        String dataSource,
        Interval interval,
        @Nullable String preferredVersion,
        int priority,
        boolean revoked
    )
    {
      this.lockType = lockType;
      this.groupId = groupId;
      this.dataSource = dataSource;
      this.interval = interval;
      this.preferredVersion = preferredVersion;
      this.priority = priority;
      this.revoked = revoked;
    }

    @Override
    public LockGranularity getGranularity()
    {
      return LockGranularity.TIME_CHUNK;
    }

    @Override
    public TaskLockType getLockType()
    {
      return lockType;
    }

    @Override
    public String getGroupId()
    {
      return groupId;
    }

    @Override
    public String getDataSource()
    {
      return dataSource;
    }

    @Override
    public Interval getInterval()
    {
      return interval;
    }

    @Nullable
    @Override
    public String getPreferredVersion()
    {
      return preferredVersion;
    }

    @Override
    public int getPriority()
    {
      return priority;
    }

    @Override
    public boolean isRevoked()
    {
      return revoked;
    }

    private String getVersion()
    {
      // Assign a new version if preferredVersion is null.
      // Assumption: We'll choose a version that is greater than any previously-chosen version for our interval. (This
      // may not always be true, unfortunately. See below.)

      final String preferredVersion = getPreferredVersion();
      if (preferredVersion != null) {
        // We have a preferred version. We'll trust our caller to not break our ordering assumptions and just use it.
        return preferredVersion;
      } else {
        // We are running under an interval lock right now, so just using the current time works as long as we can
        // trustour clock to be monotonic and have enough resolution since the last time we created a TaskLock for
        // the same interval. This may not always be true; to assure it we would need to use some method of
        // timekeeping other than the wall clock.
        return DateTimes.nowUtc().toString();
      }
    }

    @Override
    public TaskLock toLock()
    {
      return new TimeChunkLock(
          lockType,
          groupId,
          dataSource,
          interval,
          getVersion(),
          priority,
          revoked
      );
    }
  }

  public static class SegmentLockRequest implements LockRequest
  {
    private final TaskLockType lockType;
    private final String groupId;
    private final String dataSource;
    private final Interval interval;
    private final Set<Integer> partitionIds;
    private final String version;
    private final int priority;
    private final boolean revoked;

    private SegmentLockRequest(
        TaskLockType lockType,
        String groupId,
        String dataSource,
        Interval interval,
        Set<Integer> partitionIds,
        String version,
        int priority,
        boolean revoked
    )
    {
      this.lockType = lockType;
      this.groupId = groupId;
      this.dataSource = dataSource;
      this.interval = interval;
      this.partitionIds = partitionIds;
      this.version = Preconditions.checkNotNull(version, "version");
      this.priority = priority;
      this.revoked = revoked;
    }

    @Override
    public LockGranularity getGranularity()
    {
      return LockGranularity.SEGMENT;
    }

    @Override
    public TaskLockType getLockType()
    {
      return lockType;
    }

    @Override
    public String getGroupId()
    {
      return groupId;
    }

    @Override
    public String getDataSource()
    {
      return dataSource;
    }

    @Override
    public Interval getInterval()
    {
      return interval;
    }

    public Set<Integer> getPartitionIds()
    {
      return partitionIds;
    }

    @Nullable
    @Override
    public String getPreferredVersion()
    {
      return version;
    }

    @Override
    public int getPriority()
    {
      return priority;
    }

    @Override
    public boolean isRevoked()
    {
      return revoked;
    }

    @Override
    public TaskLock toLock()
    {
      return new SegmentLock(
          lockType,
          groupId,
          dataSource,
          interval,
          partitionIds,
          version,
          priority,
          revoked
      );
    }
  }

  /**
   * Perform the given action with a guarantee that the locks of the task are not revoked in the middle of action.  This
   * method first checks that all locks for the given task and intervals are valid and perform the right action.
   *
   * The given action should be finished as soon as possible because all other methods in this class are blocked until
   * this method is finished.
   *
   * @param task                   task performing a critical action
   * @param intervalToPartitionIds partitionIds which should be locked by task
   * @param action                 action to be performed inside of the critical section
   */
  public <T> T doInCriticalSection(
      Task task,
      Map<Interval, List<Integer>> intervalToPartitionIds,
      CriticalAction<T> action
  ) throws Exception
  {
    giant.lock();

    // TODO: reduce contention by checking dataSource and interval

    try {
      return action.perform(isTaskLocksValid(task, intervalToPartitionIds));
    }
    finally {
      giant.unlock();
    }
  }

  private boolean isTaskLocksValid(Task task, Map<Interval, List<Integer>> intervalToPartitionIds)
  {
    return intervalToPartitionIds
        .entrySet()
        .stream()
        .allMatch(entry -> {
          final List<TaskLockPosse> lockPosses = getOnlyTaskLockPosseContainingInterval(
              task, entry.getKey(), entry.getValue()
          );
          // Tasks cannot enter the critical section with a shared lock
          return lockPosses.stream().allMatch(
              posse -> !posse.getTaskLock().isRevoked() && posse.getTaskLock().getType() != TaskLockType.SHARED
          );
        });
  }

  private void revokeLock(TaskLockPosse lockPosse)
  {
    giant.lock();

    try {
      lockPosse.forEachTask(taskId -> revokeLock(taskId, lockPosse.getTaskLock()));
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Mark the lock as revoked. Note that revoked locks are NOT removed. Instead, they are maintained in {@link #running}
   * and {@link #taskStorage} as the normal locks do. This is to check locks are revoked when they are requested to be
   * acquired and notify to the callers if revoked. Revoked locks are removed by calling
   * {@link #unlock(Task, Interval)}.
   *
   * @param taskId an id of the task holding the lock
   * @param lock   lock to be revoked
   */
  private void revokeLock(String taskId, TaskLock lock)
  {
    giant.lock();

    try {
      if (!activeTasks.contains(taskId)) {
        throw new ISE("Cannot revoke lock for inactive task[%s]", taskId);
      }

      final Task task = taskStorage.getTask(taskId).orNull();
      if (task == null) {
        throw new ISE("Cannot revoke lock for unknown task[%s]", taskId);
      }

      log.info("Revoking task lock[%s] for task[%s]", lock, taskId);

      if (lock.isRevoked()) {
        log.warn("TaskLock[%s] is already revoked", lock);
      } else {
        final TaskLock revokedLock = lock.revokedCopy();
        taskStorage.replaceLock(taskId, lock, revokedLock);

        final List<TaskLockPosse> possesHolder = running.get(task.getDataSource()).get(lock.getInterval());
        final TaskLockPosse foundPosse = possesHolder.stream()
                                                     .filter(posse -> posse.getTaskLock().equals(lock))
                                                     .findFirst()
                                                     .orElseThrow(
                                                         () -> new ISE("Failed to find lock posse for lock[%s]", lock)
                                                     );
        possesHolder.remove(foundPosse);
        possesHolder.add(foundPosse.withTaskLock(revokedLock));
        log.info("Revoked taskLock[%s]", lock);
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Return the currently-active locks for some task.
   *
   * @param task task for which to locate locks
   * @return currently-active locks for the given task
   */
  public List<TaskLock> findLocksForTask(final Task task)
  {
    giant.lock();

    try {
      return Lists.transform(
          findLockPossesForTask(task), new Function<TaskLockPosse, TaskLock>()
          {
            @Override
            public TaskLock apply(TaskLockPosse taskLockPosse)
            {
              return taskLockPosse.getTaskLock();
            }
          }
      );
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Release lock held for a task on a particular interval. Does nothing if the task does not currently
   * hold the mentioned lock.
   *
   * @param task task to unlock
   * @param interval interval to unlock
   */
  public void unlock(final Task task, final Interval interval)
  {
    giant.lock();

    try {
      final String dataSource = task.getDataSource();
      final NavigableMap<Interval, List<TaskLockPosse>> dsRunning = running.get(task.getDataSource());

      if (dsRunning == null || dsRunning.isEmpty()) {
        return;
      }

      final List<TaskLockPosse> possesHolder = dsRunning.get(interval);
      if (possesHolder == null || possesHolder.isEmpty()) {
        return;
      }

      final List<TaskLockPosse> posses = possesHolder.stream()
                                                     .filter(posse -> posse.containsTask(task))
                                                     .collect(Collectors.toList());

      for (TaskLockPosse taskLockPosse : posses) {
        final TaskLock taskLock = taskLockPosse.getTaskLock();

        // Remove task from live list
        log.info("Removing task[%s] from TaskLock[%s]", task.getId(), taskLock.getGroupId());
        final boolean removed = taskLockPosse.removeTask(task);

        if (taskLockPosse.isTasksEmpty()) {
          log.info("TaskLock is now empty: %s", taskLock);
          possesHolder.remove(taskLockPosse);
        }

        if (possesHolder.size() == 0) {
          dsRunning.remove(interval);
        }

        if (running.get(dataSource).size() == 0) {
          running.remove(dataSource);
        }

        // Wake up blocking-lock waiters
        lockReleaseCondition.signalAll();

        // Remove lock from storage. If it cannot be removed, just ignore the failure.
        try {
          taskStorage.removeLock(task.getId(), taskLock);
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to clean up lock from storage")
             .addData("task", task.getId())
             .addData("dataSource", taskLock.getDataSource())
             .addData("interval", taskLock.getInterval())
             .addData("version", taskLock.getVersion())
             .emit();
        }

        if (!removed) {
          log.makeAlert("Lock release without acquire")
             .addData("task", task.getId())
             .addData("interval", interval)
             .emit();
        }
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Release all locks for a task and remove task from set of active tasks. Does nothing if the task is not currently locked or not an active task.
   *
   * @param task task to unlock
   */
  public void remove(final Task task)
  {
    giant.lock();
    try {
      try {
        log.info("Removing task[%s] from activeTasks", task.getId());
        for (final TaskLockPosse taskLockPosse : findLockPossesForTask(task)) {
          unlock(task, taskLockPosse.getTaskLock().getInterval());
        }
      }
      finally {
        activeTasks.remove(task.getId());
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Return the currently-active lock posses for some task.
   *
   * @param task task for which to locate locks
   */
  private List<TaskLockPosse> findLockPossesForTask(final Task task)
  {
    giant.lock();

    try {
      // Scan through all locks for this datasource
      final NavigableMap<Interval, List<TaskLockPosse>> dsRunning = running.get(task.getDataSource());
      if (dsRunning == null) {
        return ImmutableList.of();
      } else {
        return dsRunning.values().stream()
                        .flatMap(Collection::stream)
                        .filter(taskLockPosse -> taskLockPosse.containsTask(task))
                        .collect(Collectors.toList());
      }
    }
    finally {
      giant.unlock();
    }
  }

  private List<TaskLockPosse> findLockPossesContainingInterval(final String dataSource, final Interval interval)
  {
    giant.lock();

    try {
      final List<TaskLockPosse> intervalOverlapsPosses = findLockPossesOverlapsInterval(dataSource, interval);
      return intervalOverlapsPosses.stream()
                                   .filter(taskLockPosse -> taskLockPosse.taskLock.getInterval().contains(interval))
                                   .collect(Collectors.toList());
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Return all locks that overlap some search interval.
   */
  private List<TaskLockPosse> findLockPossesOverlapsInterval(final String dataSource, final Interval interval)
  {
    giant.lock();

    try {
      final NavigableMap<Interval, List<TaskLockPosse>> dsRunning = running.get(dataSource);
      if (dsRunning == null) {
        // No locks at all
        return Collections.emptyList();
      } else {
        // Tasks are indexed by locked interval, which are sorted by interval start. Intervals are non-overlapping, so:
        final NavigableSet<Interval> dsLockbox = dsRunning.navigableKeySet();
        final Iterable<Interval> searchIntervals = Iterables.concat(
            // Single interval that starts at or before ours
            Collections.singletonList(dsLockbox.floor(new Interval(interval.getStart(), DateTimes.MAX))),

            // All intervals that start somewhere between our start instant (exclusive) and end instant (exclusive)
            dsLockbox.subSet(
                new Interval(interval.getStart(), DateTimes.MAX),
                false,
                new Interval(interval.getEnd(), interval.getEnd()),
                false
            )
        );

        return StreamSupport.stream(searchIntervals.spliterator(), false)
                            .filter(searchInterval -> searchInterval != null && searchInterval.overlaps(interval))
                            .flatMap(searchInterval -> dsRunning.get(searchInterval).stream())
                            .collect(Collectors.toList());
      }
    }
    finally {
      giant.unlock();
    }
  }

  public void add(Task task)
  {
    giant.lock();
    try {
      log.info("Adding task[%s] to activeTasks", task.getId());
      activeTasks.add(task.getId());
    }
    finally {
      giant.unlock();
    }
  }

  private static boolean isAllSharedLocks(List<TaskLockPosse> lockPosses)
  {
    return lockPosses.stream()
                     .allMatch(taskLockPosse -> taskLockPosse.getTaskLock().getType().equals(TaskLockType.SHARED));
  }

  private static boolean isAllRevocable(List<TaskLockPosse> lockPosses, int tryLockPriority)
  {
    return lockPosses.stream().allMatch(taskLockPosse -> isRevocable(taskLockPosse, tryLockPriority));
  }

  private static boolean isRevocable(TaskLockPosse lockPosse, int tryLockPriority)
  {
    final TaskLock existingLock = lockPosse.getTaskLock();
    return existingLock.isRevoked() || existingLock.getNonNullPriority() < tryLockPriority;
  }

  private List<TaskLockPosse> getOnlyTaskLockPosseContainingInterval(Task task, Interval interval, List<Integer> partitionIds)
  {
    final List<TaskLockPosse> filteredPosses = findLockPossesContainingInterval(task.getDataSource(), interval)
        .stream()
        .filter(lockPosse -> lockPosse.containsTask(task))
        .collect(Collectors.toList());

    if (filteredPosses.isEmpty()) {
      throw new ISE("Cannot find locks for task[%s] and interval[%s]", task.getId(), interval);
    } else if (filteredPosses.size() > 1) {
      if (filteredPosses.stream().anyMatch(posse -> posse.getTaskLock().getGranularity() == LockGranularity.TIME_CHUNK)) {
        throw new ISE("There are multiple timeChunk lockPosses for task[%s] and interval[%s]?", task.getId(), interval);
      } else {
        final Map<Integer, TaskLockPosse> partitionIdsOfLocks = new HashMap<>();
        for (TaskLockPosse posse : filteredPosses) {
          final SegmentLock segmentLock = (SegmentLock) posse.getTaskLock();
          segmentLock.getPartitionIds().forEach(partitionId -> partitionIdsOfLocks.put(partitionId, posse));
        }

        if (partitionIds.stream().allMatch(partitionIdsOfLocks::containsKey)) {
          return partitionIds.stream().map(partitionIdsOfLocks::get).collect(Collectors.toList());
        } else {
          throw new ISE(
              "Task[%s] doesn't have locks for interval[%s] partitions[%]",
              task.getId(),
              interval,
              partitionIds.stream().filter(pid -> !partitionIdsOfLocks.containsKey(pid)).collect(Collectors.toList())
          );
        }
      }
    } else {
      return filteredPosses;
    }
  }

  @VisibleForTesting
  Set<String> getActiveTasks()
  {
    return activeTasks;
  }

  @VisibleForTesting
  Map<String, NavigableMap<Interval, List<TaskLockPosse>>> getAllLocks()
  {
    return running;
  }

  static class TaskLockPosse
  {
    private final TaskLock taskLock;
    private final Set<String> taskIds;

    TaskLockPosse(TaskLock taskLock)
    {
      this.taskLock = taskLock;
      this.taskIds = new HashSet<>();
    }

    private TaskLockPosse(TaskLock taskLock, Set<String> taskIds)
    {
      this.taskLock = taskLock;
      this.taskIds = new HashSet<>(taskIds);
    }

    TaskLockPosse withTaskLock(TaskLock taskLock)
    {
      return new TaskLockPosse(taskLock, taskIds);
    }

    TaskLock getTaskLock()
    {
      return taskLock;
    }

    boolean addTask(Task task)
    {
      if (taskLock.getType() == TaskLockType.EXCLUSIVE) {
        Preconditions.checkArgument(
            taskLock.getGroupId().equals(task.getGroupId()),
            "groupId[%s] of task[%s] is different from the existing lockPosse's groupId[%s]",
            task.getGroupId(),
            task.getId(),
            taskLock.getGroupId()
        );
      } else if (taskLock.getType() == TaskLockType.FULLY_EXCLUSIVE) {
        Preconditions.checkArgument(
            taskIds.contains(task.getId()),
            "[%s] lock can't be shared for task[%s]",
            TaskLockType.FULLY_EXCLUSIVE,
            task.getId()
        );
      }
      Preconditions.checkArgument(
          taskLock.getNonNullPriority() == task.getPriority(),
          "priority[%s] of task[%s] is different from the existing lockPosse's priority[%s]",
          task.getPriority(),
          task.getId(),
          taskLock.getNonNullPriority()
      );
      return taskIds.add(task.getId());
    }

    boolean containsTask(Task task)
    {
      Preconditions.checkNotNull(task, "task");
      return taskIds.contains(task.getId());
    }

    boolean removeTask(Task task)
    {
      Preconditions.checkNotNull(task, "task");
      return taskIds.remove(task.getId());
    }

    boolean isTasksEmpty()
    {
      return taskIds.isEmpty();
    }

    boolean reusableFor(Task task, LockRequest request)
    {
      if (taskLock.getType() == request.getLockType()) {
        switch (taskLock.getType()) {
          case SHARED:
            // All shared lock is not reusable. Instead, a new lock posse is created for all lock request.
            // See createOrFindLockPosse().
            return false;
          case EXCLUSIVE:
            return taskLock.getInterval().contains(request.getInterval()) &&
                   taskLock.getGroupId().equals(request.getGroupId());
          case FULLY_EXCLUSIVE:
            return taskLock.getInterval().contains(request.getInterval()) &&
                   taskIds.contains(task.getId());
          default:
            throw new ISE("Unknown lock type[%s]", taskLock.getType());
        }
      }

      return false;
    }

    void forEachTask(Consumer<String> action)
    {
      Preconditions.checkNotNull(action, "action");
      taskIds.forEach(action);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }

      if (!getClass().equals(o.getClass())) {
        return false;
      }

      final TaskLockPosse that = (TaskLockPosse) o;
      if (!taskLock.equals(that.taskLock)) {
        return false;
      }

      return taskIds.equals(that.taskIds);
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(taskLock, taskIds);
    }

    @Override
    public String toString()
    {
      return Objects.toStringHelper(this)
                    .add("taskLock", taskLock)
                    .add("taskIds", taskIds)
                    .toString();
    }
  }
}
