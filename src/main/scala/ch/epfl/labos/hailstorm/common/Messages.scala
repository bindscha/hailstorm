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

sealed trait HailstormMessage

sealed trait Command extends HailstormMessage {
  def bag: Bag
}

case class Create(fingerprint: FingerPrint, bag: Bag) extends Command

case class Fill(fingerprint: FingerPrint, bag: Bag, offset: Long) extends Command

case class FillSmall(fingerprint: FingerPrint, bag: Bag, offset: Long) extends Command

case class Drain(fingerprint: FingerPrint, bag: Bag, chunk: Chunk, offset: Long) extends Command

case class Trunc(fingerprint: FingerPrint, bag: Bag, size: Long) extends Command

case class Flush(fingerprint: FingerPrint, bag: Bag) extends Command

case class Delete(fingerprint: FingerPrint, bag: Bag) extends Command

sealed trait Response extends HailstormMessage

case object Ack extends Response

case object Nack extends Response

case object EOF extends Response

case class Filled(chunk: Chunk) extends Response

case class FilledSmall(chunk: SmallChunk) extends Response
