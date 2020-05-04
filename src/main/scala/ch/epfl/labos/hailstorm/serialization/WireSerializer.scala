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
package ch.epfl.labos.hailstorm.serialization

import java.nio.ByteBuffer

import akka.serialization._
import ch.epfl.labos.hailstorm.Config
import ch.epfl.labos.hailstorm.common._

object WireSerializer {
  val COMMAND_LENGTH = Config.HailstormConfig.BackendConfig.DataConfig.cmdLength
  val BAGID_LENGTH = Config.HailstormConfig.BackendConfig.DataConfig.bagIdLength
  val FINGERPRINT_LENGTH = Config.HailstormConfig.BackendConfig.DataConfig.fingerprintLength
  val CHUNKSIZE_LENGTH = Config.HailstormConfig.BackendConfig.DataConfig.chunkSizeLength
  val OFFSET_LENGTH = Config.HailstormConfig.BackendConfig.DataConfig.offsetLength
  val SIZE_LENGTH = Config.HailstormConfig.BackendConfig.DataConfig.sizeLength

  val CREATE_COMMAND = "CREA"
  val DRAIN_COMMAND = "DRAI"
  val FILL_COMMAND = "FILL"
  val FILL_SMALL_COMMAND = "SFIL"
  val FLUSH_COMMAND = "FLSH"
  val TRUNC_COMMAND = "TRUN"
  val DELETE_COMMAND = "DELE"

  val FILLED_RESPONSE = "FLLD"
  val FILLED_SMALL_RESPONSE = "SFLD"
  val ACK_RESPONSE = "ACK!"
  val NACK_RESPONSE = "NACK"
  val EOF_RESPONSE = "EOF!"
}

class WireSerializer extends Serializer {

  import WireSerializer._

  // This is whether "fromBinary" requires a "clazz" or not
  def includeManifest: Boolean = true

  // Pick a unique identifier for your Serializer,
  // you've got a couple of billions to choose from,
  // 0 - 16 is reserved by Akka itself
  def identifier = 31415926

  // "toBinary" serializes the given object to an Array of Bytes
  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case Create(fingerprint, bag) =>
      val buf = ByteBuffer.wrap(new Array[Byte](COMMAND_LENGTH + BAGID_LENGTH + FINGERPRINT_LENGTH))
      buf.put(CREATE_COMMAND.getBytes)
      buf.put(fingerprint.getBytes)
      buf.put(bag.id.getBytes)
      buf.array
    case Drain(fingerprint, bag, chunk, offset) =>
      chunk.cmd(DRAIN_COMMAND)
      chunk.fingerprint(fingerprint)
      chunk.bag(bag)
      chunk.offset(offset)
      chunk.array
    case Fill(fingerprint, bag, offset) =>
      val buf = ByteBuffer.wrap(new Array[Byte](COMMAND_LENGTH + BAGID_LENGTH + FINGERPRINT_LENGTH + OFFSET_LENGTH))
      buf.put(FILL_COMMAND.getBytes)
      buf.put(fingerprint.getBytes)
      buf.put(bag.id.getBytes)
      buf.putLong(offset)
      buf.array
    case FillSmall(fingerprint, bag, offset) =>
      val buf = ByteBuffer.wrap(new Array[Byte](COMMAND_LENGTH + BAGID_LENGTH + FINGERPRINT_LENGTH + OFFSET_LENGTH))
      buf.put(FILL_SMALL_COMMAND.getBytes)
      buf.put(fingerprint.getBytes)
      buf.put(bag.id.getBytes)
      buf.putLong(offset)
      buf.array
    case Flush(fingerprint, bag) =>
      val buf = ByteBuffer.wrap(new Array[Byte](COMMAND_LENGTH + BAGID_LENGTH + FINGERPRINT_LENGTH))
      buf.put(FLUSH_COMMAND.getBytes)
      buf.put(fingerprint.getBytes)
      buf.put(bag.id.getBytes)
      buf.array
    case Trunc(fingerprint, bag, size) =>
      val buf = ByteBuffer.wrap(new Array[Byte](COMMAND_LENGTH + BAGID_LENGTH + FINGERPRINT_LENGTH + SIZE_LENGTH))
      buf.put(TRUNC_COMMAND.getBytes)
      buf.put(fingerprint.getBytes)
      buf.put(bag.id.getBytes)
      buf.putLong(size)
      buf.array
    case Delete(fingerprint, bag) =>
      val buf = ByteBuffer.wrap(new Array[Byte](COMMAND_LENGTH + BAGID_LENGTH + FINGERPRINT_LENGTH))
      buf.put(DELETE_COMMAND.getBytes)
      buf.put(fingerprint.getBytes)
      buf.put(bag.id.getBytes)
      buf.array
    case Filled(chunk) =>
      chunk.cmd(FILLED_RESPONSE)
      chunk.array
    case FilledSmall(chunk) =>
      chunk.cmd(FILLED_SMALL_RESPONSE)
      chunk.array
    case Ack =>
      ACK_RESPONSE.getBytes
    case Nack =>
      NACK_RESPONSE.getBytes
    case EOF =>
      EOF_RESPONSE.getBytes
  }

  // "fromBinary" deserializes the given array,
  // using the type hint (if any, see "includeManifest" above)
  def fromBinary(
                  bytes: Array[Byte],
                  clazz: Option[Class[_]]): AnyRef = {
    if (bytes.length >= Config.HailstormConfig.BackendConfig.DataConfig.chunkLength) {
      val chunk = Chunk.wrap(bytes)
      chunk.cmd match {
        case DRAIN_COMMAND => Drain(chunk.fingerprint, chunk.bag, chunk, chunk.offset)
        case FILLED_RESPONSE => Filled(chunk)
        case other => throw new RuntimeException("Unknown large message received! " + other)
      }
    } else if (bytes.length >= Config.HailstormConfig.BackendConfig.DataConfig.smallChunkLength) {
      val chunk = SmallChunk.wrap(bytes)
      chunk.cmd match {
        case FILLED_SMALL_RESPONSE => FilledSmall(chunk)
        case other => throw new RuntimeException("Unknown large message received! " + other)
      }
    } else {
      val buf = ByteBuffer.wrap(bytes)
      val cmd = new Array[Byte](COMMAND_LENGTH)
      buf.get(cmd)
      new String(cmd) match {
        case CREATE_COMMAND =>
          val fingerprintBytes = new Array[Byte](FINGERPRINT_LENGTH)
          buf.get(fingerprintBytes)
          val bagBytes = new Array[Byte](BAGID_LENGTH)
          buf.get(bagBytes)
          Create(new String(fingerprintBytes).trim, Bag(new String(bagBytes).trim))
        case FILL_COMMAND =>
          val fingerprintBytes = new Array[Byte](FINGERPRINT_LENGTH)
          buf.get(fingerprintBytes)
          val bagBytes = new Array[Byte](BAGID_LENGTH)
          buf.get(bagBytes)
          val offset = buf.getLong(COMMAND_LENGTH + BAGID_LENGTH + FINGERPRINT_LENGTH)
          Fill(new String(fingerprintBytes).trim, Bag(new String(bagBytes).trim), offset)
        case FILL_SMALL_COMMAND =>
          val fingerprintBytes = new Array[Byte](FINGERPRINT_LENGTH)
          buf.get(fingerprintBytes)
          val bagBytes = new Array[Byte](BAGID_LENGTH)
          buf.get(bagBytes)
          val offset = buf.getLong(COMMAND_LENGTH + BAGID_LENGTH + FINGERPRINT_LENGTH)
          FillSmall(new String(fingerprintBytes).trim, Bag(new String(bagBytes).trim), offset)
        case FLUSH_COMMAND =>
          val fingerprintBytes = new Array[Byte](FINGERPRINT_LENGTH)
          buf.get(fingerprintBytes)
          val bagBytes = new Array[Byte](BAGID_LENGTH)
          buf.get(bagBytes)
          Flush(new String(fingerprintBytes).trim, Bag(new String(bagBytes).trim))
        case TRUNC_COMMAND =>
          val fingerprintBytes = new Array[Byte](FINGERPRINT_LENGTH)
          buf.get(fingerprintBytes)
          val bagBytes = new Array[Byte](BAGID_LENGTH)
          buf.get(bagBytes)
          val size = buf.getLong(COMMAND_LENGTH + BAGID_LENGTH + FINGERPRINT_LENGTH)
          Trunc(new String(fingerprintBytes).trim, Bag(new String(bagBytes).trim), size)
        case DELETE_COMMAND =>
          val fingerprintBytes = new Array[Byte](FINGERPRINT_LENGTH)
          buf.get(fingerprintBytes)
          val bagBytes = new Array[Byte](BAGID_LENGTH)
          buf.get(bagBytes)
          Delete(new String(fingerprintBytes).trim, Bag(new String(bagBytes).trim))
        case ACK_RESPONSE => Ack
        case NACK_RESPONSE => Nack
        case EOF_RESPONSE => EOF
      }
    }
  }
}

