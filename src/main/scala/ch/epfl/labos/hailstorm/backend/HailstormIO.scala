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
package ch.epfl.labos.hailstorm.backend

import java.io.{File => _, _}

import akka.actor._
import better.files.File
import ch.epfl.labos.hailstorm._
import ch.epfl.labos.hailstorm.common._
import ch.epfl.labos.hailstorm.frontend._
import net.smacke.jaydio._

object HailstormIO {

  def props(bag: Bag, root: File): Props =
    Config.HailstormConfig.BackendConfig.DataConfig.ioEngine match {
      case Config.HailstormConfig.BackendConfig.DataConfig.DefaultIOEngine =>
        Props(classOf[HailstormIO], bag.id, root).withDispatcher("hailstorm.backend.blocking-io-dispatcher")
      case Config.HailstormConfig.BackendConfig.DataConfig.DirectIOEngine =>
        Props(classOf[HailstormDIO], bag.id, root).withDispatcher("hailstorm.backend.blocking-io-dispatcher")
    }

  sealed trait Operation

  case object Read extends Operation

  case object Write extends Operation

  case object Noop extends Operation

}

class HailstormIO(bag: Bag, root: File = File(Config.HailstormConfig.BackendConfig.DataConfig.dataDirectory.getAbsolutePath)) extends Actor with ActorLogging {

  import HailstormIO._

  // Create root if not exists
  root.createDirectories()

  private var inout = new RandomAccessFile((root / bag.id).toJava, "rw")

  def receive = {
    case Create(fingerprint, file) =>
    // do nothing
    case Fill(fingerprint, file, offset) =>
      val buffer = BackendChunkPool.allocate()
      val read = withStats(Read) {
        inout.seek(offset)
        val ret = inout.read(buffer.array, 0, Config.HailstormConfig.BackendConfig.DataConfig.chunkLength)
        ret
      }
      if (read >= 0) {
        buffer.chunkSize(read)
        sender ! Filled(buffer)
      } else {
        sender ! EOF
      }
    case FillSmall(fingerprint, file, offset) =>
      val buffer = BackendChunkPool.newSmallChunk()
      val read = withStats(Read) {
        inout.seek(offset)
        val ret = inout.read(buffer.array, 0, Config.HailstormConfig.BackendConfig.DataConfig.smallChunkLength)
        ret
      }
      if (read >= 0) {
        buffer.chunkSize(read)
        sender ! FilledSmall(buffer)
      } else {
        sender ! EOF
      }
    case Drain(fingerprint, file, data, offset) =>
      withStats(Write) {
        inout.seek(offset)
        inout.write(data.array, 0, data.chunkSize)
      }
      sender ! Ack
    case Trunc(fingerprint, file, size) =>
      withStats(Noop) {
        inout.getChannel.truncate(size)
      }
      sender ! Ack
    case Flush(fingerprint, file) =>
      withStats(Noop) {
        inout.getFD.sync()
      }
      sender ! Ack
    case Delete(fingerprint, file) =>
      withStats(Noop) {
        inout.close()
        (root / bag.id).delete(true)
      }
      sender ! Ack
  }

  def withStats[A](op: Operation)(f: => A): A = {
    val started = System.nanoTime()
    val ret = f
    Statistics.ioTime send (_ + (System.nanoTime - started))
    op match {
      case Read if ret.asInstanceOf[Int] > 0 => Statistics.chunksRead send (_ + 1)
      case Write => Statistics.chunksWritten send (_ + 1)
      case _ =>
    }
    ret
  }

}

class HailstormDIO(bag: Bag, root: File = File(Config.HailstormConfig.BackendConfig.DataConfig.dataDirectory.getAbsolutePath)) extends Actor with ActorLogging {

  import HailstormIO._

  // Create root if not exists
  root.createDirectories()

  private var inout = new DirectRandomAccessFile((root / bag.id).toJava, "rw", 4 * 1024 * 1024)

  // XXX: read is a problem if it does not have exactly the right amount (e.g., last chunk of file)

  def receive = {
    case Create(fingerprint, file) =>
    // do nothing
    case Fill(fingerprint, file, offset) =>
      val buffer = BackendChunkPool.allocate()
      val fp = inout.getFilePointer
      withStats(Read) {
        inout.seek(offset)
        inout.read(buffer.array, 0, Config.HailstormConfig.BackendConfig.DataConfig.chunkLength)
        inout.seek(fp)
        buffer.array.length
      }
      sender ! Filled(buffer)
    case Drain(fingerprint, file, data, offset) =>
      withStats(Write) {
        inout.seek(offset)
        inout.write(data.array, 0, data.chunkSize)
        Config.HailstormConfig.BackendConfig.DataConfig.chunkLength
      }
      sender ! Ack
    case Trunc(fingerprint, file, size) =>
      withStats(Noop) {
        inout.close()
        (root / bag.id).fileChannel.get.truncate(size)
        inout = new DirectRandomAccessFile((root / bag.id).toJava, "rw", 4 * 1024 * 1024)
      }
      sender ! Ack
    case Flush(fingerprint, file) => // no need to flush, but we still ack it
      sender ! Ack
    case Delete(fingerprint, file) =>
      withStats(Noop) {
        inout.close()
        (root / bag.id).delete(true)
      }
      sender ! Ack
  }

  def withStats[A](op: Operation)(f: => A): A = {
    val started = System.nanoTime()
    val ret = f
    Statistics.ioTime send (_ + (System.nanoTime - started))
    op match {
      case Read if ret.asInstanceOf[Int] > 0 => Statistics.chunksRead send (_ + 1)
      case Write => Statistics.chunksWritten send (_ + 1)
      case _ =>
    }
    ret
  }

}
