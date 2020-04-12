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

package org.apache.samza.container

import org.apache.samza.task.CoordinatorRequests
import org.apache.samza.system.{IncomingMessageEnvelope, SystemConsumers, SystemStreamPartition}
import org.apache.samza.task.ReadableCoordinator
import org.apache.samza.util.{Logging, Throttleable, ThrottlingExecutor, TimerUtil}

import scala.collection.JavaConverters._

/**
  * The run loop uses a single-threaded execution model: activities for
  * all {@link TaskInstance}s within a container are multiplexed onto one execution
  * thread. Those activities include task callbacks (such as StreamTask.process and
  * WindowableTask.window), committing checkpoints, etc.
  *
  * <p>This class manages the execution of that run loop, determining what needs to
  * be done when.
  */
class RunLoop (
                val taskInstances: Map[TaskName, TaskInstance],
                val consumerMultiplexer: SystemConsumers,
                val metrics: SamzaContainerMetrics,
                val maxThrottlingDelayMs: Long,
                val windowMs: Long = -1,
                val commitMs: Long = 60000,
                val clock: () => Long = { System.nanoTime }) extends Runnable with Throttleable with TimerUtil with Logging {

  private val metricsMsOffset = 1000000L
  private val executor = new ThrottlingExecutor(maxThrottlingDelayMs)
  private var lastWindowNs = clock()
  private var lastCommitNs = clock()
  private var activeNs = 0L
  private var tuples = 0
  private var latency = 0L
  private var startTime = 0L



  @volatile private var shutdownNow = false
  private val coordinatorRequests: CoordinatorRequests = new CoordinatorRequests(taskInstances.keySet.asJava)

  // Messages come from the chooser with no connection to the TaskInstance they're bound for.
  // Keep a mapping of SystemStreamPartition to TaskInstance to efficiently route them.
  val systemStreamPartitionToTaskInstances = getSystemStreamPartitionToTaskInstancesMapping

  def getSystemStreamPartitionToTaskInstancesMapping: Map[SystemStreamPartition, List[TaskInstance]] = {
    // We could just pass in the SystemStreamPartitionMap during construction,
    // but it's safer and cleaner to derive the information directly
    def getSystemStreamPartitionToTaskInstance(taskInstance: TaskInstance) =
      taskInstance.systemStreamPartitions.map(_ -> taskInstance).toMap

    taskInstances.values
      .flatMap(getSystemStreamPartitionToTaskInstance)
      .groupBy(_._1)
      .map { case (ssp, ssp2taskInstance) => ssp -> ssp2taskInstance.map(_._2).toList }
  }

  /**
    * Starts the run loop. Blocks until either the tasks request shutdown, or an
    * unhandled exception is thrown.
    */
  def run {
    var start = clock()
    var processTime = 0L
    var timeInterval = 0L
    var chooseTime = 0L;

    while (!shutdownNow) {
      var prevNs = clock()

      trace("Attempting to choose a message to process.")

      // Exclude choose time from activeNs. Although it includes deserialization time,
      // it most closely captures idle time.
      val chooseStart = clock()
      val envelope = updateTimer(metrics.chooseNs) {
        consumerMultiplexer.choose()
      }
      val chooseNs = clock() - chooseStart

      val processStart = clock()

      executor.execute(new Runnable() {
        override def run(): Unit = process(envelope)
      })

      window
      commit

      val currentNs = clock()
      val totalNs = currentNs - prevNs
      // need to add deserialization ns, this is non trivial when processing time is short
      // we also need to exempt those time spent on commit and window when there is no tuple input.
      var usefulTime = currentNs - processStart
      if (envelope != null) {
        usefulTime += chooseNs
      } else {
        chooseTime += chooseNs
        usefulTime = 0
      }

      if (totalNs != 0) {
        metrics.utilization.set(activeNs.toFloat / totalNs)
      }

      processTime += usefulTime
      timeInterval += totalNs

      if (currentNs - start >= 1000000000) { // totalNs is not 0 if timer metrics are enabled
        val utilization = processTime.toFloat / timeInterval
        val idleTime = chooseTime.toFloat / timeInterval
        val serviceRate = tuples.toFloat*1000 / (processTime)
        val avgLatency = if (tuples == 0) 0
        else latency / tuples.toFloat
        //          log.debug("utilization: " + utilization + " tuples: " + tuples + " service rate: " + serviceRate + " average latency: " + avgLatency);
        println("utilization: " + utilization + " tuples: " + tuples + " service rate: " + serviceRate + " average latency: " + avgLatency
          + " chooseNs: " + idleTime)
        metrics.avgUtilization.set(utilization)
        metrics.serviceRate.set(serviceRate)
        metrics.latency.set(avgLatency)
        start = currentNs
        processTime = 0L
        timeInterval = 0L
        tuples = 0
        latency = 0
        chooseTime = 0
      }

      activeNs = 0L
    }
  }

  def setWorkFactor(workFactor: Double): Unit = executor.setWorkFactor(workFactor)

  def getWorkFactor: Double = executor.getWorkFactor

  def shutdown: Unit = {
    shutdownNow = true
  }

  /**
    * Chooses a message from an input stream to process, and calls the
    * process() method on the appropriate StreamTask to handle it.
    */
  private def process(envelope: IncomingMessageEnvelope) {
    metrics.processes.inc

    activeNs += updateTimerAndGetDuration(metrics.processNs) ((currentTimeNs: Long) => {
      if (envelope != null) {
        tuples += 1
        if (startTime == 0) {
          startTime = System.currentTimeMillis()
          println("start time: " + startTime)
        }

        val ssp = envelope.getSystemStreamPartition

        trace("Processing incoming message envelope for SSP %s." format ssp)
        metrics.envelopes.inc

        val taskInstances = systemStreamPartitionToTaskInstances(ssp)
        taskInstances.foreach {
          taskInstance =>
          {
            val coordinator = new ReadableCoordinator(taskInstance.taskName)
            taskInstance.process(envelope, coordinator)
            coordinatorRequests.update(coordinator)
          }
        }
        // latency should be the time when the tuple has been processed - envelope timestamp.
        latency += System.currentTimeMillis() - envelope.getTimestamp
        println("stock_id: " + ssp.getPartition.getPartitionId + " arrival_ts: " + envelope.getTimestamp + " completion_ts: " + System.currentTimeMillis())
      } else {
        trace("No incoming message envelope was available.")
        metrics.nullEnvelopes.inc
      }
    })
  }

  /**
    * Invokes WindowableTask.window on all tasks if it's time to do so.
    */
  private def window {
    activeNs += updateTimerAndGetDuration(metrics.windowNs) ((currentTimeNs: Long) => {
      if (windowMs >= 0 && lastWindowNs + windowMs * metricsMsOffset < currentTimeNs) {
        trace("Windowing stream tasks.")
        lastWindowNs = currentTimeNs
        metrics.windows.inc

        taskInstances.foreach {
          case (taskName, task) =>
            val coordinator = new ReadableCoordinator(taskName)
            task.window(coordinator)
            coordinatorRequests.update(coordinator)
        }
      }
    })
  }

  /**
    * Commits task state as a a checkpoint, if necessary.
    */
  private def commit {
    activeNs += updateTimerAndGetDuration(metrics.commitNs) ((currentTimeNs: Long) => {
      if (commitMs >= 0 && lastCommitNs + commitMs * metricsMsOffset < currentTimeNs) {
        info("Committing task instances because the commit interval has elapsed.")
        lastCommitNs = currentTimeNs
        metrics.commits.inc
        taskInstances.values.foreach(_.commit)
      } else if (!coordinatorRequests.commitRequests.isEmpty){
        trace("Committing due to explicit commit request.")
        metrics.commits.inc
        coordinatorRequests.commitRequests.asScala.foreach(taskName => {
          taskInstances(taskName).commit
        })
      }

      shutdownNow |= coordinatorRequests.shouldShutdownNow
      coordinatorRequests.commitRequests.clear()
    })
  }
}
