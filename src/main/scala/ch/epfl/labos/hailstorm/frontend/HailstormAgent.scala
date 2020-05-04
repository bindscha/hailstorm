/*
 * Copyright (c) 2020 EPFL IC LABOS.
 *
 * This file is part of Hailstorm
 * (see https://labos.epfl.ch/hailstorm).
 *
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
package ch.epfl.labos.hailstorm.frontend

import java.lang.management.ManagementFactory

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import ch.epfl.labos.hailstorm.Config
import com.sun.management.OperatingSystemMXBean
import better.files._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.collection.mutable.{Map => MMap}
import scala.util.Success
import scala.sys.process._

object HailstormAgent {

  val name: String = "agent"

  def props(rootDirectory: String): Props = Props(classOf[HailstormAgent], rootDirectory)

  case object Tick

  case class CpuLoad(value: Double)

  case class CompactionOffloading(id: Long, toCompact: List[HailstormStorageManager.FileMetadata])
  case class CompactionDone(id: Long, compacted: List[HailstormStorageManager.FileMetadata])

}

class HailstormAgent(rootDirectory: String) extends Actor with ActorLogging {

  import HailstormAgent._
  import context.dispatcher

  implicit val timeout = Timeout(2.seconds)

  var timer: Option[Cancellable] = None
  var startT: Long = 0

  val cpuLoads: MMap[ActorRef, Double] = MMap(Config.HailstormConfig.FrontendConfig.NodesConfig.agentRefs.map(_ -> 0.0) :_*)

  val os = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
  var cpuLoad: Double = 0.0

  val rootDir = File(rootDirectory)

  override def preStart(): Unit = {
    super.preStart()
    log.info("Hailstorm Agent started!")

    timer = Some(context.system.scheduler.schedule(Duration.Zero, Config.HailstormConfig.FrontendConfig.schedulerTick, self, Tick))
  }

  override def postStop(): Unit = {
    log.info("Hailstorm Agent stopped!")
    timer map (_.cancel())

    context.system.terminate()

    super.postStop()
  }

  override def receive = {
    case Tick =>
      if ((rootDir / "rocksdb").exists) {
        val currentCpuUsage = math.min(os.getProcessCpuLoad * 2, 1) // *2 adjusts for hyperthreading
        cpuLoad = Config.HailstormConfig.FrontendConfig.alpha * currentCpuUsage + (1 - Config.HailstormConfig.FrontendConfig.alpha) * cpuLoad
        cpuLoads.keys.foreach(_ ! CpuLoad(cpuLoad))

        val compactions = (rootDir / "rocksdb").list.filter(f => f.name.endsWith(".req"))
        for (c <- compactions) {
          val cid = c.name.replace(".req", "").replace("offload-", "").toLong
          c.delete(true) // Remove the request so we don't compact several times...
          val sstables = c.contentAsString.split('\n').tail.map(sst => (rootDir / "rocksdb" / sst).pathAsString).toList
          val metadataF = Future.sequence(sstables.map(sst => (HailstormFrontendFuse.storageManager ? HailstormStorageManager.GetMetadata(sst)).mapTo[HailstormStorageManager.FileMetadata]))
          Await.ready(metadataF, Duration.Inf)
          val sstablesMetadata = metadataF.value match {
            case Some(Success(metadata)) => metadata
            case _ => throw new RuntimeException("Could not get metadata for files: " + sstables)
          }
          val (node, targetCpu) = cpuLoads.min

          if (cpuLoad - targetCpu > Config.HailstormConfig.FrontendConfig.theta) {
            node ! CompactionOffloading(cid, sstablesMetadata)
          } else {
            self ! CompactionOffloading(cid, sstablesMetadata)
          }
        }
      }

    case CpuLoad(value) =>
      cpuLoads += sender -> value

    case CompactionOffloading(cid, toCompact) =>
      val createdF = Future.sequence(toCompact.map(metadata => (HailstormFrontendFuse.storageManager ? HailstormStorageManager.FileCreate((rootDir / "rocksdb" / metadata.path).pathAsString, metadata.uuid)).mapTo[HailstormStorageManager.FileCreated.type]))
      Await.ready(createdF, Duration.Inf)
      (rootDir / s"compaction-$cid").createDirectory()
      for (metadata <- toCompact) {
        (rootDir / "rocksdb" / metadata.path).moveToDirectory(rootDir / s"compaction-$cid")
      }

      val process = s"compaction-offloading ${(rootDir / s"compaction-$cid").pathAsString}".run()
      val processF = Future(process.exitValue())
      Await.ready(processF, Duration.Inf)

      val sstables = (rootDir / s"compaction-$cid").list.filter(f => f.name.endsWith(".sst")).map(_.pathAsString).toList
      val metadataF = Future.sequence(sstables.map(sst => (HailstormFrontendFuse.storageManager ? HailstormStorageManager.GetMetadata(sst)).mapTo[HailstormStorageManager.FileMetadata]))
      Await.ready(metadataF, Duration.Inf)
      val sstablesMetadata = metadataF.value match {
        case Some(Success(metadata)) => metadata.toList
        case _ => throw new RuntimeException("Could not get metadata for files: " + sstables)
      }
      sender ! CompactionDone(cid, sstablesMetadata)

    case CompactionDone(id, compacted) =>
      val createdF = Future.sequence(compacted.map(metadata => (HailstormFrontendFuse.storageManager ? HailstormStorageManager.FileCreate((rootDir / "rocksdb" / metadata.path).pathAsString, metadata.uuid)).mapTo[HailstormStorageManager.FileCreated.type]))
      Await.ready(createdF, Duration.Inf)
      (rootDir / s"offload-$id.done").touch()
      // RocksDB takes over from here
  }

}
