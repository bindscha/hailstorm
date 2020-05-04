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
package ch.epfl.labos.hailstorm.common

import ch.epfl.labos.hailstorm.Config

object FrontendChunkPool extends ChunkPool {

  override protected val nchunks: Int = (Config.HailstormConfig.FrontendConfig.chunkPoolSize / Chunk.dataSize).toInt

}

object BackendChunkPool extends ChunkPool {

  override protected val nchunks: Int = (Config.HailstormConfig.BackendConfig.chunkPoolSize / Chunk.dataSize).toInt

}

trait ChunkPool {

  protected var chunks: List[Chunk] = Nil

  def init(): Unit = {
    chunks = (for (i <- 0 until nchunks) yield newChunk()).toList
  }

  def newChunk(): Chunk = {
    val ret = new RichChunk(new Array(Chunk.size))
    ret.chunkSize(Chunk.dataSize)
    ret
  }

  def allocate(): Chunk = chunks match {
    case chunk :: cs => chunks = cs; chunk
    case _ => newChunk()
  }

  def deallocate(chunk: Chunk): Unit = {
    // XXX: this is not the smartest way to do this
    java.util.Arrays.fill(chunk.array, 0.toByte)
    chunks = chunks ::: List(chunk)
  }

  def available: Int = chunks.size

  protected def nchunks: Int

  def newSmallChunk(): SmallChunk = {
    val ret = new RichSmallChunk(new Array(SmallChunk.size))
    ret.chunkSize(SmallChunk.dataSize)
    ret
  }

}
