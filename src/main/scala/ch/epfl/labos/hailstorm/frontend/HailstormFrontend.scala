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

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import ch.epfl.labos.hailstorm.common._
import ch.epfl.labos.hailstorm.frontend.HailstormFileHandle.PathChanged
import ch.epfl.labos.hailstorm.util._
import ch.epfl.labos.hailstorm.{CliArguments, Config}
import com.typesafe.config._
import jnr.ffi.Pointer
import jnr.ffi.types.{off_t, size_t}

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.Success

import java.sql.{Array => _, _}

object HailstormFrontendFuse {

  var system: ActorSystem = null
  var storageManager: ActorRef = null
  var agent: ActorRef = null

  def start(cliArguments: CliArguments): Unit = {
    var config =
      if (cliArguments.me.isSupplied) {
        //Config.HailstormConfig.me = arguments.me()
        Config.HailstormConfig.FrontendConfig.frontendConfig
          .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.nodes(cliArguments.me()).hostname))
          .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.nodes(cliArguments.me()).port))
          .withValue("akka.remote.netty.tcp.bind-hostname", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.nodes(cliArguments.me()).hostname))
          .withValue("akka.remote.netty.tcp.bind-port", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.nodes(cliArguments.me()).port))
          .withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.nodes(cliArguments.me()).hostname))
          .withValue("akka.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.nodes(cliArguments.me()).port))
          .withValue("akka.remote.artery.bind.hostname", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.nodes(cliArguments.me()).hostname))
          .withValue("akka.remote.artery.bind.port", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.nodes(cliArguments.me()).port))
      } else {
        Config.HailstormConfig.FrontendConfig.frontendConfig
          .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.localNode.hostname))
          .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.localNode.port))
          .withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.localNode.hostname))
          .withValue("akka.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(Config.HailstormConfig.FrontendConfig.NodesConfig.localNode.port))
      }

    if (cliArguments.verbose()) {
      config = config.withValue("akka.loglevel", ConfigValueFactory.fromAnyRef("DEBUG"))
    }

    system = ActorSystem("HailstormFrontend", config)

    Config.HailstormConfig.BackendConfig.NodesConfig.connectBackend(system)

    storageManager = system.actorOf(HailstormStorageManager.props(cliArguments.fileMappingDb(), cliArguments.clearOnInit()), HailstormStorageManager.name)

    if (cliArguments.offloading()) {
      agent = system.actorOf(HailstormAgent.props(cliArguments.mountpoint()), HailstormAgent.name)
    }

    system.registerOnTermination {
      System.exit(0)
    }

    system.log.debug("Allocating buffers...")
    FrontendChunkPool.init()
  }

  case class Broadcast(req: Any)

  case class RequestWrapper(req: Any, sender: ActorRef, retries: Int = 3)

  case class RequestAborted(req: Any, reason: Throwable)

  case object Processed

}

object HailstormStorageManager {

  val name: String = "roxxfs"
  val INVALID_CHARS_MAP =
    Map(
      "/" -> "!"
    )
  val INVALID_CHARS_REVERSE_MAP =
    INVALID_CHARS_MAP.map(_.swap)

  def props(fileMappingDb: String, clearOnInit: Boolean): Props = Props(classOf[HailstormStorageManager], fileMappingDb, clearOnInit)

  def encodePath(path: String): String = java.util.UUID.nameUUIDFromBytes(path.getBytes).toString

  sealed trait StorageCommands

  sealed trait StorageResponse

  case class OpenFile(path: String) extends StorageCommands

  case class CloseFile(path: String) extends StorageCommands

  case class RenameFile(path: String, newPath: String) extends StorageCommands

  case class DeleteFile(path: String) extends StorageCommands

  case class ListFiles(path: String) extends StorageCommands

  case class FileOpened(ref: ActorRef) extends StorageResponse

  case class FilesList(ls: List[String]) extends StorageResponse

  case object FileClosed extends StorageResponse

  case object FileRenamed extends StorageResponse

  case object FileDeleted extends StorageResponse

  case class FileCreate(path: String, uuid: String) extends StorageCommands

  case object FileCreated extends StorageResponse

  case class GetMetadata(path: String) extends StorageCommands

  case class FileMetadata(path: String, uuid: String) extends StorageResponse

  /*def encodePath(path: String): String = {
    var ret = path
    for((old, _new) <- INVALID_CHARS_MAP) {
      ret = ret.replace(old, _new)
    }
    ret
  }

  def decodePath(path: String): String = {
    var ret = path
    for((old, _new) <- INVALID_CHARS_REVERSE_MAP) {
      ret = ret.replace(old, _new)
    }
    ret
  }*/

}

class FileTable(fileMappingDb: String, clearOnInit: Boolean = true, pathMap: MMap[String, String] = MMap.empty, openFilesMap: MMap[String, ActorRef] = MMap.empty)(implicit context: ActorContext) {

  val dbUrl = f"jdbc:sqlite:$fileMappingDb"
  val connection = DriverManager.getConnection(dbUrl)

  connection.prepareStatement("CREATE TABLE IF NOT EXISTS paths (uuid TEXT PRIMARY KEY, path TEXT)").execute()
  if (clearOnInit) {
    connection.prepareStatement("DELETE FROM paths").executeUpdate()
  } else {
    load()
  }

  def open(path: String): ActorRef =
    pathMap get path match {
      case Some(uuid) =>
        openFilesMap get uuid match {
          case Some(ref) => ref
          case _ =>
            val ref = context.actorOf(HailstormFileHandle.props(uuid, path), HailstormFileHandle.name(uuid))
            openFilesMap += uuid -> ref
            persist(path, uuid)
            ref
        }
      case _ =>
        val uuid = java.util.UUID.randomUUID().toString
        pathMap += path -> uuid
        val ref = context.actorOf(HailstormFileHandle.props(uuid, path), HailstormFileHandle.name(uuid))
        openFilesMap += uuid -> ref
        persist(path, uuid)
        ref
    }

  def delete(path: String): Unit = {
    close(path)
    pathMap -= path
    unpersist(path)
  }

  def close(path: String): Unit = {
    pathMap get path foreach (openFilesMap -= _)
  }

  def rename(oldPath: String, newPath: String): Unit =
    pathMap get oldPath match {
      case Some(uuid) =>
        pathMap -= oldPath
        pathMap += newPath -> uuid
        unpersist(oldPath)
        persist(newPath, uuid)
      case _ =>
        // Apparently this scenario is possible and should work... (cf. RocksDB's mv of a ghost MANIFEST file)
        val uuid = java.util.UUID.randomUUID().toString
        pathMap += newPath -> uuid
        persist(newPath, uuid)
    }

  def get(path: String): Option[ActorRef] =
    pathMap get path flatMap (openFilesMap get _)

  def paths: List[String] = pathMap.keys.toList

  def exists(path: String): Boolean = pathMap contains path

  def isOpen(path: String): Boolean =
    pathMap get path flatMap openFilesMap.get isDefined

  def load() = {
    val paths: ResultSet = connection.prepareStatement("SELECT * FROM paths").executeQuery()
    while(paths.next()) {
      val uuid = paths.getString("uuid")
      val path = paths.getString("path")
      pathMap += path -> uuid
    }
  }

  def persist(path: String, uuid: String) = {
    val existsSql = connection.prepareStatement("SELECT * FROM paths WHERE uuid=? AND path=?")
    existsSql.setString(1, uuid)
    existsSql.setString(2, path)
    if (!existsSql.executeQuery().next()) {
      val insertSql = connection.prepareStatement("INSERT INTO paths(uuid, path) VALUES (?, ?)")
      insertSql.setString(1, uuid)
      insertSql.setString(2, path)
      insertSql.executeUpdate()
    }
  }

  def unpersist(path: String) = {
    val deleteSql = connection.prepareStatement("DELETE FROM paths WHERE path=?")
    deleteSql.setString(1, path)
    deleteSql.executeUpdate()
  }

  def metadata(path: String): String =
    pathMap get path match {
      case Some(uuid) => uuid
      case _ =>
        val uuid = java.util.UUID.randomUUID().toString
        pathMap += path -> uuid
        uuid
    }

  def create(path: String, uuid: String): Unit =
    pathMap += path -> uuid
}

class HailstormStorageManager(fileMappingDb: String, clearOnInit: Boolean) extends Actor with ActorLogging {

  import HailstormStorageManager._

  val ft = new FileTable(fileMappingDb, clearOnInit)

  override def preStart(): Unit = {
    super.preStart()

    ft.load()

    log.debug("Started!")
  }

  override def receive: Receive = {
    case OpenFile(path) =>
      log.debug(s"OPEN $path")
      val ref = ft open path
      sender ! FileOpened(ref)
    case CloseFile(path) =>
      log.debug(s"CLOSE $path")
      ft close path
      sender ! FileClosed
    case RenameFile(path, newPath) =>
      log.debug(s"MV $path -> $newPath")
      ft.rename(path, newPath)
      sender ! FileRenamed
      ft.get(path) foreach (_ ! PathChanged(newPath))
    case DeleteFile(path) =>
      log.debug(s"DELETE $path")
      val ref = ft open path
      ref ! HailstormFileHandle.Delete
      ft delete path
      sender ! FileDeleted
    case ListFiles(path) =>
      log.debug(s"LS $path")
      val ls = ft.paths.filter(_.startsWith(path)).map(_.substring(path.length)).filterNot(_.startsWith(".")).map(_.takeWhile(_ != '/'))
      sender ! FilesList(ls)
    case GetMetadata(path) =>
      log.debug(s"META $path")
      val metadata = ft metadata path
      sender ! FileMetadata(path, metadata)
    case FileCreate(path, uuid) =>
      log.debug(s"CREAT $path")
      ft create (path, uuid)
      sender ! FileCreated
  }
}


object HailstormFileHandle {

  def name(id: String): String = s"fp-$id"

  def props(id: String, path: String): Props = Props(classOf[HailstormFileHandle], id, path)

  implicit val timeout = Timeout(30 seconds)

  sealed trait FileCommands

  sealed trait FileResponse

  sealed trait InternalMessages

  sealed trait CacheState

  case class Read(buf: Pointer, @size_t size: Long, @off_t offset: Long) extends FileCommands

  case class Write(buf: Pointer, @size_t size: Long, @off_t offset: Long) extends FileCommands

  case class Trunc(@size_t size: Long) extends FileCommands

  case class FileAttr(@size_t size: Long) extends FileResponse

  case class ReadAck(@size_t size: Long) extends FileResponse

  case class WriteAck(@size_t size: Long) extends FileResponse

  case class Failure(reason: Option[String] = None) extends FileResponse

  case class Writeback(offset: Long) extends InternalMessages

  case class PathChanged(newPath: String) extends InternalMessages

  case object GetAttr extends FileCommands

  case object Flush extends FileCommands

  case object Delete extends FileCommands

  case object TruncAck extends FileResponse

  case object FlushAck extends FileResponse

  case object Ack extends FileResponse

  case object Nack extends FileResponse

  case object Valid extends CacheState

  case object Dirty extends CacheState

}

class HailstormFileHandle(id: String, var path: String) extends Actor with ActorLogging {

  import HailstormFileHandle._
  import context.dispatcher

  implicit val materializer = akka.stream.ActorMaterializer()

  val file = id
  val cache = MMap.empty[Long, Chunk]
  val cacheState = MMap.empty[Long, CacheState]
  val offsetShift = if (Config.HailstormConfig.BackendConfig.DataConfig.chunkIsPowerOfTwo) (Math.log(Chunk.dataSize) / Math.log(2)).toInt else -1
  var fileSize = 0L
  var client: ActorRef = null

  override def preStart(): Unit = {
    super.preStart()

    client = context.actorOf(HailstormBagClient.props(Bag(id), path), HailstormBagClient.name(Bag(id)))

    log.debug(s"Started file handle for $path")
  }

  override def postStop(): Unit = {
    log.debug(s"Stopped file handle for $path")

    super.postStop()
  }

  override def receive: Receive = {
    case GetAttr =>
      log.debug(s"STAT $path (${FileAttr(fileSize)})")

      sender ! FileAttr(fileSize)

    case Read(buf, size, offset) =>
      log.debug(s"READ $path (${size}b)")

      if (size <= SmallChunk.dataSize && Config.HailstormConfig.FrontendConfig.smallReadsEnabled) {
        val chunkF = (client ? HailstormBagClient.ReadSmall(offset)).mapTo[HailstormBagClient.ReadSmallAcked].map(_.chunk)
        Await.ready(chunkF, 30 seconds)

        chunkF.value match {
          case Some(Success(chunk)) =>
            buf.put(0L, chunk.array, (offset % SmallChunk.dataSize).toInt, size.toInt)
            sender ! ReadAck(size)
          case error =>
            log.warning(s"Something unexpectedly went wrong reading $size bytes from $file from offset $offset!")
            sender ! Failure(Some(s"Something unexpectedly went wrong reading $size bytes from $file from offset $offset! + $error"))
        }
      } else {
        val chunksF: Future[List[Chunk]] =
          Future.sequence(
            List.range(alignedOffset(offset), Math.min(offset + size, fileSize), Chunk.dataSize) map { off =>
              (hasChunk(off) map (c => Future(c))) getOrElse (client ? HailstormBagClient.Read(off)).mapTo[HailstormBagClient.ReadAcked].map(_.chunk)
            })
        Await.ready(chunksF, 30 seconds)

        chunksF.value match {
          case Some(Success(chunks)) if chunks.isEmpty =>
            sender ! ReadAck(0)
          case Some(Success(chunks)) =>
            var off = offset
            var leftToRead: Long = size
            var i = 0
            do {
              val chunk = chunks(i)
              val chunkOff = (off % Chunk.dataSize).toInt
              val copySize = Math.min(leftToRead, Chunk.dataSize - chunkOff).toInt
              buf.put(size - leftToRead, chunk.array, chunkOff, copySize)
              off += copySize
              leftToRead -= copySize
              i += 1
            } while (leftToRead > 0)

            sender ! ReadAck(size)
          case error =>
            log.warning(s"Something unexpectedly went wrong reading $size bytes from $file from offset $offset!")
            sender ! Failure(Some(s"Something unexpectedly went wrong reading $size bytes from $file from offset $offset! + $error"))
        }
      }

    case Write(buf, size, offset) =>
      log.debug(s"WRITE $path (${size}b at $offset)")

      var off = offset
      var leftToWrite: Long = size
      do {
        val chunk = getChunk(off)
        val chunkOff = chunkOffset(off)
        val copySize = Math.min(leftToWrite, Chunk.dataSize - chunkOff).toInt
        buf.get(size - leftToWrite, chunk.array, chunkOff, copySize)
        chunk.chunkSize(chunkOff + copySize)
        markAs(off, Dirty)
        if (chunkOff == Chunk.dataSize) {
          self ! Writeback(off)
        }
        off += copySize
        leftToWrite -= copySize
      } while (leftToWrite > 0)

      fileSize = Math.max(fileSize, size + offset)
      log.debug(s"$path new size is $fileSize")

      sender ! WriteAck(size)

    case Trunc(size) => // Blocking implementation
      log.debug(s"TRUNC $path (${size}b)")

      cache.keys filter (_ > size) foreach { off => cache -= off; cacheState -= off }
      fileSize = Math.min(size, fileSize)

      var success = false
      do {
        val f = (client ? HailstormBagClient.Trunc(size)).mapTo[HailstormBagClient.TruncAcked.type]
        Await.ready(f, 30 seconds)
        f.value match {
          case Some(Success(HailstormBagClient.TruncAcked)) => success = true
          case _ => log.warning("Trunc failed! Trying again...")
        }
      } while (!success)

      sender ! TruncAck

    /*case Flush => // Blocking implementation
      log.debug(s"FLUSH $path")

      val dirty = cache filterKeys (o => cacheState.get(o).contains(Dirty))

      var success = false
      do {
        val writeF: Future[List[HailstormBagClient.WriteAcked]] =
          Future.sequence(dirty map (e => (client ? HailstormBagClient.Write(e._1, e._2)).mapTo[HailstormBagClient.WriteAcked]) toList)
        Await.ready(writeF, 30 seconds)

        writeF.value match {
          case Some(Success(writtenBack)) => writtenBack.map(_.offset).foreach(cacheState += _ -> Valid); success = true
          case _ => log.warning("Writeback failed! Trying again...")
        }
      } while (!success)

      success = false
      do {
        val f = (client ? HailstormBagClient.Flush).mapTo[HailstormBagClient.FlushAcked.type]
        Await.ready(f, 30 seconds)
        f.value match {
          case Some(Success(HailstormBagClient.FlushAcked)) => success = true
          case _ => log.warning("Flush failed! Trying again...")
        }
      } while (!success)

      sender ! FlushAck*/

    case Flush =>
      log.debug(s"FLUSH $path")

      val dirty = cache.keysIterator filter (o => cacheState.get(o).contains(Dirty))
      dirty foreach (self ! Writeback(_))

      sender ! FlushAck

    case Delete => // Blocking implementation
      log.debug(s"DELETE $path")

      cache.clear()
      cacheState.clear()
      fileSize = 0

      var success = false
      do {
        val f = (client ? HailstormBagClient.Delete).mapTo[HailstormBagClient.Deleted.type]
        Await.ready(f, 30 seconds)
        f.value match {
          case Some(Success(HailstormBagClient.Deleted)) => success = true
          case _ => log.warning("Delete failed! Trying again...")
        }
      } while (!success)

      context stop self

    case Writeback(offset) =>
      val aligned = alignedOffset(offset)
      val chunk = getChunk(offset)
      val me = self
      client ? HailstormBagClient.Write(aligned, chunk) foreach (me ! _)

    case HailstormBagClient.WriteAcked(offset) =>
      log.debug(s"Successful writeback at $offset")
      markAs(offset, Valid)

    case PathChanged(newPath) =>
      path = newPath
      client ! HailstormBagClient.PathChanged(newPath)
  }

  private def hasChunk(offset: Long) =
    cache.get(alignedOffset(offset))

  private def alignedOffset(offset: Long): Long =
    if (offsetShift >= 0) {
      (offset >>> offsetShift) << offsetShift
    } else {
      offset / Chunk.dataSize * Chunk.dataSize
    }

  private def getChunk(offset: Long) =
    cache.getOrElseUpdate(alignedOffset(offset), newChunk)

  private def newChunk: Chunk = {
    val chunk: Chunk = FrontendChunkPool.allocate()
    chunk.chunkSize(0)
    chunk
  }

  private def markAs(offset: Long, state: CacheState) =
    cacheState += alignedOffset(offset) -> state

  private def chunkOffset(offset: Long): Int =
    if (offsetShift >= 0) {
      (offset & ((1 << offsetShift) - 1)).toInt
    } else {
      (offset % Chunk.dataSize).toInt
    }

}


object HailstormBagClient {

  def name(bag: Bag): String = s"client"

  def props(bag: Bag, path: String): Props = Props(classOf[HailstormBagClient], bag.id, path) // Unpacking manually because bag is value class

  implicit val timeout = Timeout(Config.HailstormConfig.FrontendConfig.sinkRetry)

  sealed trait BagCommand

  sealed trait BagResponse

  sealed trait InternalMessage

  case class Read(offset: Long) extends BagCommand

  case class ReadSmall(offset: Long) extends BagCommand

  case class Write(offset: Long, chunk: Chunk) extends BagCommand

  case class Trunc(size: Long) extends BagCommand

  case class ReadAcked(offset: Long, chunk: Chunk) extends BagResponse

  case class ReadSmallAcked(offset: Long, chunk: SmallChunk) extends BagResponse

  case class ReadEOF(offset: Long) extends BagResponse

  case class WriteAcked(offset: Long) extends BagResponse

  case class Acked(id: ActorCounter.Id, chunkOpt: Option[Chunk] = None) extends InternalMessage

  case class AckedSmall(id: ActorCounter.Id, chunkOpt: Option[SmallChunk] = None) extends InternalMessage

  case class Nacked(id: ActorCounter.Id) extends InternalMessage

  case class Retry(id: ActorCounter.Id) extends InternalMessage

  case class PathChanged(newPath: String)

  case object Flush extends BagCommand

  case object Delete extends BagCommand

  case object TruncAcked extends BagResponse

  case object FlushAcked extends BagResponse

  case object Deleted extends BagResponse

}


class HailstormBagClient(bag: Bag, var path: String) extends Actor with ActorCounter with ActorLogging with Stash {

  import HailstormBagClient._
  import ch.epfl.labos.hailstorm.common
  import context.dispatcher

  val cyclic =
    Config.HailstormConfig.FrontendConfig.ioMode match {
      case Config.HailstormConfig.FrontendConfig.InputLocal => Cyclic[ActorRef](7 * bag.id.hashCode + 13 * Config.HailstormConfig.me, Config.HailstormConfig.BackendConfig.NodesConfig.backendRefs)
      case Config.HailstormConfig.FrontendConfig.OutputLocal => Cyclic[ActorRef](1, scala.collection.immutable.Seq(Config.HailstormConfig.BackendConfig.NodesConfig.backendRefs(bag.id.hashCode % Config.HailstormConfig.BackendConfig.NodesConfig.machines)))
      case Config.HailstormConfig.FrontendConfig.Spreading => Cyclic[ActorRef](7 * bag.id.hashCode + 13 * Config.HailstormConfig.me, Config.HailstormConfig.BackendConfig.NodesConfig.backendRefs)
    }

  val fingerprint = java.util.UUID.nameUUIDFromBytes(Config.HailstormConfig.FrontendConfig.NodesConfig.localNode.toString.getBytes).toString

  var queue = Map.empty[ActorCounter.Id, (BagCommand, ActorRef)]

  override def preStart(): Unit = {
    super.preStart()

    log.debug(s"Started bag client for $path")
  }

  override def postStop(): Unit = {
    log.debug(s"Stopped bag client for $path")

    super.postStop()
  }

  override def receive = {
    case Acked(id, chunkOpt) =>
      queue get id match {
        case Some((r: Read, sendTo)) if chunkOpt.isDefined => sendTo ! ReadAcked(r.offset, chunkOpt.get)
        case Some((r: Read, sendTo)) => sendTo ! ReadEOF(r.offset)
        case Some((w: Write, sendTo)) => sendTo ! WriteAcked(w.offset)
        case _ => log.debug(s"Got acknowledgement for unknown operation $id!") // Not warning since writes can be replaced
      }
      queue -= id

    case Retry(id) =>
      log.debug(s"Retrying $id for bag ${bag.id} ($path) due to failure...")
      queue get id match {
        case Some((r: Read, _)) => fillChunk(id, r)
        case Some((w: Write, _)) => drainChunk(id, w)
        case _ => log.warning(s"Got retry for unknown operation $id!")
      }

    case r@Read(offset) =>
      val id = nextId
      queue += id -> ((r, sender))
      fillChunk(id, r)

    case AckedSmall(id, chunkOpt) =>
      queue get id match {
        case Some((r: ReadSmall, sendTo)) if chunkOpt.isDefined => sendTo ! ReadSmallAcked(r.offset, chunkOpt.get)
        case Some((r: ReadSmall, sendTo)) => sendTo ! ReadEOF(r.offset)
        case _ => log.debug(s"Got acknowledgement for unknown operation $id!") // Not warning since writes can be replaced
      }
      queue -= id

    case r@ReadSmall(offset) =>
      val id = nextId
      queue += id -> ((r, sender))
      fillChunkSmall(id, r)

    case w@Write(offset, chunk) =>
      // Check to see if we don't have a pending write for the same chunk
      // If so, we drop it from the queue, so it gets dropped when the answer comes back
      // In theory, this ensures the latest write takes precedence over older pending writes
      // Note: this does not fully guarantee the above property as network can reorder messages (however unlikely given
      // TCP and a single switch between nodes), so the only correct solution would be to timestamp messages (e.g., by
      // sending Id along) so the server can do the right thing.
      queue.find(e => e._2._1 match {
        case Write(o, _) if offset == o => true;
        case _ => false
      }) match {
        case Some(pendingWrite) => queue -= pendingWrite._1
        case _ => // do nothing
      }
      val id = nextId
      queue += id -> ((w, sender))
      drainChunk(id, w)

    case Trunc(size) =>
      val beyondSize = queue.filter(_._2._1 match { case Write(o, _) if o + Chunk.dataSize > size => true; case Read(o) if o + Chunk.dataSize > size => true; case _ => false })
      if (beyondSize nonEmpty) {
        log.warning(s"Pending message queue contained operations beyond new bag size when received TRUNC command! Clearing: $beyondSize")
        beyondSize.keys foreach (queue -= _)
      }

      val lowSize: Long = targetOffset(size)
      val highSize: Long = lowSize + Chunk.dataSize
      val idxBump: Int = ((size - lowSize * cyclic.size) / Chunk.dataSize).toInt
      val bumpSize: Long = lowSize + size % Chunk.dataSize

      val ops = (Seq.fill(idxBump)(highSize) ++ Seq(bumpSize) ++ Seq.fill(cyclic.size - idxBump - 1)(lowSize)) map (s => common.Trunc(fingerprint, bag, s))

      val replyTo = sender
      runGlobalOp("trunc", ops, cyclic.permutation, () => replyTo ! TruncAcked)

    case Flush =>
      val (pendingWrites, pendingReads) = queue.partition(_._2._1.isInstanceOf[Write])
      if (pendingWrites nonEmpty) {
        log.warning(s"Pending message queue contained writes when received FLUSH command! Clearing: $pendingWrites")
        queue = pendingReads
      }

      val replyTo = sender
      runGlobalOp("flush", Seq.fill(cyclic.size)(common.Flush(fingerprint, bag)), cyclic.permutation, () => replyTo ! FlushAcked)

    case Delete =>
      if (queue.nonEmpty) {
        log.warning(s"Pending message queue was not empty when received DELETE command! Clearing: $queue")
        queue = Map.empty
      }

      val replyTo = sender
      runGlobalOp("delete", Seq.fill(cyclic.size)(common.Delete(fingerprint, bag)), cyclic.permutation, () => replyTo ! Deleted)

    case HailstormBagClient.PathChanged(newPath) =>
      path = newPath
  }

  def waitForGlobalOp(doneCallback: () => Unit): Receive = {
    case Terminated(actor) =>
      if (actor.path.name == HailstormGlobalOperation.name) {
        doneCallback()
        unstashAll()
        context become receive
      } else {
        log.warning("Got terminated message for unknown actor!")
      }

    case _ => stash()
  }

  def runGlobalOp(name: String, ops: Seq[Command], nodes: Seq[ActorRef], doneCallback: () => Unit = () => {}) = {
    log.debug(s"Running global operation $name...")
    val worker = context.actorOf(HailstormGlobalOperation.props(ops, nodes), HailstormGlobalOperation.name)
    context watch worker
    context become waitForGlobalOp(doneCallback)
  }

  def fillChunk(id: ActorCounter.Id, read: Read): Unit = {
    val me = self
    (target(read.offset) ? Fill(fingerprint, bag, targetOffset(read.offset))) onComplete {
      case Success(Filled(chunk)) => me ! Acked(id, Some(chunk))
      case Success(EOF) => me ! Acked(id) // no chunk = EOF
      case msg => me ! Retry(id)
    }
  }

  def fillChunkSmall(id: ActorCounter.Id, read: ReadSmall): Unit = {
    val me = self
    (target(read.offset) ? FillSmall(fingerprint, bag, targetOffset(read.offset))) onComplete {
      case Success(FilledSmall(chunk)) => me ! AckedSmall(id, Some(chunk))
      case Success(EOF) => me ! AckedSmall(id) // no chunk = EOF
      case msg => me ! Retry(id)
    }
  }

  def drainChunk(id: ActorCounter.Id, write: Write): Unit = {
    val me = self
    (target(write.offset) ? Drain(fingerprint, bag, write.chunk, targetOffset(write.offset))) onComplete {
      case Success(Ack) => me ! Acked(id)
      case _ => me ! Retry(id)
    }
  }

  private def target(offset: Long): ActorRef =
    cyclic.permutation((offset / Chunk.dataSize).toInt % cyclic.size)

  private def targetOffset(offset: Long): Long =
    (offset / Chunk.dataSize) / cyclic.size * Chunk.dataSize

}

object HailstormGlobalOperation {

  val name: String = "global-op"

  def props(ops: Seq[Command], backends: Seq[ActorRef]): Props = Props(classOf[HailstormGlobalOperation], ops, backends)

  case class Acked(id: Int)

  case object Tick

  implicit val timeout = Timeout(5.seconds)

}

class HailstormGlobalOperation(ops: Seq[Command], backends: Seq[ActorRef]) extends Actor with ActorLogging {

  import HailstormGlobalOperation._
  import context.dispatcher

  require(ops.size == backends.size, "Operations list must be the same size as backends list!")

  val ready = Array.fill(backends.size)(false)

  var timer: Option[Cancellable] = None

  override def preStart(): Unit = {
    log.debug("Started global op")

    timer = Some(context.system.scheduler.schedule(Duration.Zero, Config.HailstormConfig.FrontendConfig.syncBarrierRetry, self, Tick))
  }

  override def postStop(): Unit = {
    timer map (_.cancel())

    log.debug("Finished global op")
  }

  override def receive = {
    case Acked(id) if id >= 0 && id < ready.length =>
      ready(id) = true
      if (ready forall identity) { // received ready from all
        context stop self
      }

    case Tick =>
      ready.zipWithIndex filter (_._1 == false) foreach { p =>
        val id = p._2
        val me = self
        (backends(id) ? ops(id)) map { case Ack => me ! Acked(id) }
      }
  }

}

