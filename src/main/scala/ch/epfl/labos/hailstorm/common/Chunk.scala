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

import java.nio._

import ch.epfl.labos.hailstorm.Config

object Chunk {

  val dataSize: Int = Config.HailstormConfig.BackendConfig.DataConfig.chunkLength

  val metaSize: Int = Config.HailstormConfig.BackendConfig.DataConfig.metaLength

  val size: Int = dataSize + metaSize

  def wrap(bytes: Array[Byte]): Chunk = new RichChunk(bytes)

}

object ChunkMeta {

  val chunkSizeSize: Int = Config.HailstormConfig.BackendConfig.DataConfig.chunkSizeLength
  val cmdSize: Int = Config.HailstormConfig.BackendConfig.DataConfig.cmdLength
  val fingerprintSize: Int = Config.HailstormConfig.BackendConfig.DataConfig.fingerprintLength
  val bagSize: Int = Config.HailstormConfig.BackendConfig.DataConfig.bagIdLength
  val offsetSize: Int = Config.HailstormConfig.BackendConfig.DataConfig.offsetLength

  val chunkSizeOffset: Int = Config.HailstormConfig.BackendConfig.DataConfig.chunkLength
  val cmdOffset: Int = Config.HailstormConfig.BackendConfig.DataConfig.chunkLength + chunkSizeSize
  val fingerprintOffset: Int = Config.HailstormConfig.BackendConfig.DataConfig.chunkLength + chunkSizeSize + cmdSize
  val bagOffset: Int = Config.HailstormConfig.BackendConfig.DataConfig.chunkLength + chunkSizeSize + cmdSize + fingerprintSize
  val offsetOffset: Int = Config.HailstormConfig.BackendConfig.DataConfig.chunkLength + chunkSizeSize + cmdSize + fingerprintSize + bagSize

}

trait ChunkMeta extends Any {
  this: Chunk =>

  import ChunkMeta._

  def chunkSize(size: Int): Unit = {
    val buf = ByteBuffer.wrap(array, chunkSizeOffset, chunkSizeSize)
    buf.putInt(size)
  }

  def chunkSize: Int = {
    val buf = ByteBuffer.wrap(array, chunkSizeOffset, chunkSizeSize)
    buf.getInt
  }

  def cmd(cmd: String): Unit = {
    cmdOffset until cmdOffset + cmdSize foreach (i => array(i) = 0)
    val bytes = cmd.getBytes
    val length = if (bytes.length < cmdSize) bytes.length else cmdSize
    System.arraycopy(bytes, 0, array, cmdOffset, length)
  }

  def cmd: String = {
    val ret = new Array[Byte](cmdSize)
    System.arraycopy(array, cmdOffset, ret, 0, cmdSize)
    new String(ret).trim
  }

  def fingerprint(fingerprint: FingerPrint): Unit = {
    fingerprintOffset until fingerprintOffset + fingerprintSize foreach (i => array(i) = 0)
    val bytes = bag.id.getBytes
    val length = if (bytes.length < fingerprintSize) bytes.length else fingerprintSize
    System.arraycopy(bytes, 0, array, fingerprintOffset, length)
  }

  def bag: Bag = {
    val ret = new Array[Byte](bagSize)
    System.arraycopy(array, bagOffset, ret, 0, bagSize)
    Bag(new String(ret).trim)
  }

  def fingerprint: FingerPrint = {
    val ret = new Array[Byte](fingerprintSize)
    System.arraycopy(array, fingerprintOffset, ret, 0, fingerprintSize)
    new String(ret).trim
  }

  def bag(bag: Bag): Unit = {
    bagOffset until bagOffset + bagSize foreach (i => array(i) = 0)
    val bytes = bag.id.getBytes
    val length = if (bytes.length < bagSize) bytes.length else bagSize
    System.arraycopy(bytes, 0, array, bagOffset, length)
  }

  def offset(offset: Long): Unit = {
    val buf = ByteBuffer.wrap(array, offsetOffset, offsetSize)
    buf.putLong(offset)
  }

  def offset: Long = {
    val buf = ByteBuffer.wrap(array, offsetOffset, offsetSize)
    buf.getLong
  }

}

trait Chunk extends Any with ChunkMeta {

  def array: Array[Byte]

  def asByteBuffer: ByteBuffer = ByteBuffer.wrap(array, 0, chunkSize)

}
