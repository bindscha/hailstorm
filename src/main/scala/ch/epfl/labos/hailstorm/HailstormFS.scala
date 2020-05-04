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
package ch.epfl.labos.hailstorm

import java.nio.file.Paths

import akka.pattern.ask
import akka.util.Timeout
import ch.epfl.labos.hailstorm.backend._
import ch.epfl.labos.hailstorm.common._
import ch.epfl.labos.hailstorm.frontend._
import jnr.ffi.types.{mode_t, off_t, size_t}
import jnr.ffi.{Platform, Pointer}
import org.rogach.scallop._
import ru.serce.jnrfuse.struct.{FileStat, FuseFileInfo, Statvfs}
import ru.serce.jnrfuse.{ErrorCodes, FuseFillDir, FuseStubFS}
import uk.org.lidalia.sysoutslf4j.context._

import scala.collection._
import scala.collection.mutable.{Set => MSet}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util._

class CliArguments(arguments: Seq[String]) extends ScallopConf(arguments) {
  version("Hailstorm 1.0.0 (c) 2020 Laurent Bindschaedler - LABOS - EPFL")

  val me = opt[Int](default = None, descr = "Node ID")
  val client = opt[Boolean](short = 'c', default = Some(true), descr = "Run client")
  val daemon = opt[Boolean](short = 'd', default = Some(true), descr = "Run daemon")
  val mountpoint = opt[String](short = 'm', default = Some("/mnt/hailstorm"), descr = "Where to mount the filesystem")
  val blacklist = opt[List[String]](short = 'b', default = Some(Nil), descr = "Blacklist matching files and folders")
  val blacklistPath = opt[String](short = 'p', descr = "Where to put blacklisted files (should be outside HailstormFS mountpoint)")
  val offloading = opt[Boolean](short = 'l', default = Some(true), descr = "Enable Hailstorm Agent for compaction offloading")
  val fileMappingDb = opt[String](short = 'f', default = Some("./roxxfs.sqlite"), descr = "Where to put the persistent file mapping database")
  val clearOnInit = opt[Boolean](short = 'i', default = Some(true), descr = "Drop persistent file mapping at startup")
  val fuseOpts = opt[List[String]](short = 'o', default = Some(Nil), descr = "FUSE options")
  val verbose = opt[Boolean](short = 'v', default = Some(false), descr = "Enable debug logging")

  override def onError(e: Throwable) = e match {
    case exceptions.ScallopException(message) =>
      println(message)
      printHelp
      sys.exit(-1)
    case ex => super.onError(ex)
  }

  dependsOnAny(blacklist, List(blacklistPath))
  verify()
}

object HailstormFS {
  def main(args: Array[String]): Unit = {
    SysOutOverSLF4J.sendSystemOutAndErrToSLF4J(LogLevel.DEBUG, LogLevel.DEBUG)
    val log = org.slf4j.LoggerFactory.getLogger(classOf[HailstormFS])

    val cliArguments = new CliArguments(args)

    log.info(
      """
        |______  __      ___________      _____                        _________________
        |___  / / /_____ ___(_)__  /________  /____________________ ______  ____/_  ___/
        |__  /_/ /_  __ `/_  /__  /__  ___/  __/  __ \_  ___/_  __ `__ \_  /_   _____ \
        |_  __  / / /_/ /_  / _  / _(__  )/ /_ / /_/ /  /   _  / / / / /  __/   ____/ /
        |/_/ /_/  \__,_/ /_/  /_/  /____/ \__/ \____//_/    /_/ /_/ /_//_/      /____/
        |
        |
        |Copyright (c) 2020 Laurent Bindschaedler - LABOS - EPFL
        |""".stripMargin)

    log.info("Initializing Hailstorm...")
    val hfs = new HailstormFS(cliArguments)

    if (cliArguments.blacklist.isSupplied) {
      log.info(s"Blacklist is ON. Files matching '${cliArguments.blacklist().mkString(", ")}' will go to ${cliArguments.blacklistPath()}")
    }

    Config.ModeConfig.mode match {
      case Config.ModeConfig.Dev =>
        // Testing: starting 3 backend nodes and 1 frontend node
        for (i <- 0 until 3) {
          val newArgs = (args.toSeq ++ Seq("--me", s"$i")).toArray // need to force --me here
          HailstormBackend.start(new CliArguments(newArgs))
        }
        HailstormFrontendFuse.start(cliArguments)
      case Config.ModeConfig.Prod =>
        // Prod: starting 1 backend node and 1 frontend node
        HailstormBackend.start(cliArguments)
        Thread.sleep(5000) // sleep 5 seconds to make sure the backend is started
        HailstormFrontendFuse.start(cliArguments)
    }

    hfs.rootDirectory.loadPersistedFiles()

    try {
      val path: String = cliArguments.mountpoint()
      var fuseOpts: Seq[String] = Seq.empty
      // OS-specific config
      Platform.getNativePlatform.getOS match {
        case Platform.OS.WINDOWS =>
          fuseOpts = fuseOpts ++ Seq.empty
        case Platform.OS.DARWIN =>
          fuseOpts = fuseOpts ++ Seq(s"-oiosize=${Chunk.dataSize}")
        case _ =>
          fuseOpts = fuseOpts ++ Seq("-ononempty", "-obig_writes", s"-omax_read=${Chunk.dataSize}", s"-omax_write=${Chunk.dataSize}")
      }
      fuseOpts = fuseOpts ++ cliArguments.fuseOpts().map("-o" + _)
      log.info(s"Mounting HailstormFS to path $path with options $fuseOpts")
      hfs.mount(Paths.get(path), true, cliArguments.verbose(), fuseOpts.toArray)
    } finally hfs.umount()
  }
}

class HailstormFS(cliArguments: CliArguments) extends FuseStubFS {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[HailstormFS])
  val rootDirectory = new HailstormDirectory("")

  override def init(conn: Pointer): Pointer = {
    super.init(conn)
  }

  override def create(path: String, @mode_t mode: Long, fi: FuseFileInfo): Int =
    getPath(path) match {
      case Some(_) => -ErrorCodes.EEXIST
      case _ =>
        val isBlacklisted =
          try {
            cliArguments.blacklist().exists(path.matches)
          } catch {
            case _: Throwable => log.warn("Warning! Blacklist patterns fail when matching! This is likely caused by a malformed regex."); false
          }
        if (isBlacklisted) {
          symlink(cliArguments.blacklistPath() + '/' + basename(path), path)
          0
        } else {
          getParent(path) match {
            case Some(parent) => parent add basename(path); 0
            case _ => -ErrorCodes.ENOENT
          }
        }
    }

  override def getattr(path: String, stat: FileStat): Int =
    getPath(path) match {
      case Some(p) => p.getattr(stat); 0
      case _ => -ErrorCodes.ENOENT
    }

  override def mkdir(path: String, @mode_t mode: Long): Int =
    getPath(path) match {
      case Some(_) => -ErrorCodes.EEXIST
      case _ =>
        getParent(path) match {
          case Some(parent) => parent add new HailstormDirectory(basename(path), Some(parent)); 0
          case _ => -ErrorCodes.ENOENT
        }
    }

  override def rmdir(path: String): Int = unlink(path)

  override def unlink(path: String): Int =
    getPath(path) match {
      case Some(p) => p.delete(); 0
      case _ => -ErrorCodes.ENOENT
    }

  override def readdir(path: String, buf: Pointer, filler: FuseFillDir, @off_t offset: Long, fi: FuseFileInfo): Int =
    getPath(path) match {
      case Some(dir: HailstormDirectory) =>
        filler.apply(buf, ".", null, 0)
        filler.apply(buf, "..", null, 0)
        dir.readdir(buf, filler)
        0
      case Some(file: HailstormFile) => -ErrorCodes.ENOTDIR
      case _ => -ErrorCodes.ENOENT
    }

  /*rootDirectory add "file1.txt"
  rootDirectory add "file 2.txt"

  val subDirectory1 = new HailstormDirectory("dir 1", Some(rootDirectory))
  rootDirectory add subDirectory1
  subDirectory1 add "a file"

  val subDirectory2 = new HailstormDirectory("dir 2", Some(rootDirectory))
  rootDirectory add subDirectory2

  val subsubDirectory1 = new HailstormDirectory("sub dir 1", Some(subDirectory1))
  subDirectory1 add subsubDirectory1
  subsubDirectory1 add "a file"
  subsubDirectory1 add "another file"
  subsubDirectory1 add "yet another file"*/

  override def rename(path: String, newName: String): Int =
    getPath(path) match {
      case Some(p) =>
        getParent(newName) match {
          case Some(dir: HailstormDirectory) =>
            withLock {
              p.parent.foreach(_.rem(p))
              p.rename(basename(newName))
              dir add p
            }
            0
          case _ => -ErrorCodes.ENOENT
        }
      case _ => -ErrorCodes.ENOENT
    }

  def getPath(path: String): Option[HailstormPath] = getPath(str2path(path))

  def getParent(path: String): Option[HailstormDirectory] = getPath(str2path(path).dropRight(1)).map(_.asInstanceOf[HailstormDirectory])

  def getPath(path: List[String]): Option[HailstormPath] = rootDirectory.find(path)

  def str2path(path: String): List[String] = path.split(HailstormPath.SEPARATOR).filter(_.nonEmpty).toList

  def basename(path: String): String = str2path(path).last

  def withLock[T](thunk: => T): T =
    if (Config.HailstormConfig.FrontendConfig.lockFsMetadata) rootDirectory.synchronized(thunk)
    else thunk

  override def statfs(path: String, stbuf: Statvfs): Int = {
    if (Platform.getNativePlatform.getOS eq Platform.OS.WINDOWS) {
      // statfs needs to be implemented on Windows in order to allow for copying
      // data from other devices because winfsp calculates the volume size based
      // on the statvfs call.
      // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
      if ("/" == path) {
        stbuf.f_blocks.set(1024 * 1024) // total data blocks in file system
        stbuf.f_frsize.set(1024) // fs block size
        stbuf.f_bfree.set(1024 * 1024) // free blocks in fs

      }
    }
    super.statfs(path, stbuf)
  }

  override def open(path: String, fi: FuseFileInfo): Int = {
    getPath(path) match {
      case Some(dir: HailstormDirectory) => -ErrorCodes.EISDIR
      case Some(file: HailstormFile) => file.open()
      case _ => -ErrorCodes.ENOENT
    }
  }

  override def read(path: String, buf: Pointer, @size_t size: Long, @off_t offset: Long, fi: FuseFileInfo): Int = {
    getPath(path) match {
      case Some(dir: HailstormDirectory) => -ErrorCodes.EISDIR
      case Some(file: HailstormFile) => file.read(buf, size, offset, fi)
      case _ => -ErrorCodes.ENOENT
    }
  }

  override def write(path: String, buf: Pointer, @size_t size: Long, @off_t offset: Long, fi: FuseFileInfo): Int = {
    getPath(path) match {
      case Some(dir: HailstormDirectory) => -ErrorCodes.EISDIR
      case Some(file: HailstormFile) => file.write(buf, size, offset, fi)
      case _ => -ErrorCodes.ENOENT
    }
  }

  override def truncate(path: String, @size_t size: Long): Int = {
    getPath(path) match {
      case Some(dir: HailstormDirectory) => -ErrorCodes.EISDIR
      case Some(file: HailstormFile) => file.truncate(size)
      case _ => -ErrorCodes.ENOENT
    }
  }

  override def flush(path: String, fi: FuseFileInfo): Int = {
    getPath(path) match {
      case Some(dir: HailstormDirectory) => -ErrorCodes.EISDIR
      case Some(file: HailstormFile) => file.flush()
      case _ => -ErrorCodes.ENOENT
    }
  }

  override def fallocate(path: String, mode: Int, off: Long, length: Long, fi: FuseFileInfo): Int = 0

  override def symlink(oldpath: String, newpath: String): Int =
    getPath(newpath) match {
      case Some(_) => -ErrorCodes.EEXIST
      case _ =>
        getParent(newpath) match {
          case Some(parent) => parent add new HailstormLink(basename(newpath), oldpath, Some(parent)); 0
          case _ => -ErrorCodes.ENOENT
        }
    }

  override def readlink(path: String, buf: Pointer, size: Long): Int =
    getPath(path) match {
      case Some(dir: HailstormDirectory) => -ErrorCodes.EISDIR
      case Some(file: HailstormFile) => -ErrorCodes.EEXIST
      case Some(link: HailstormLink) =>
        val copySize = Math.min(link.target.getBytes.length, size).toInt // don't copy more than size bytes anyways
        buf.put(0, link.target.getBytes, 0, copySize)
        buf.putByte(copySize, 0.toByte) // put a 0-terminated string in the buffer
        0
      case _ => -ErrorCodes.ENOENT
    }

  sealed trait HailstormPath {
    def name: String

    def parent: Option[HailstormDirectory]

    def getattr(stat: FileStat): Unit

    def delete(): Unit

    def rename(newName: String): Unit

    def find(path: List[String]): Option[HailstormPath]

    def path: List[String] = parent match {
      case Some(p) => p.path ::: List(name)
      case _ => Nil
    }
  }

  class HailstormDirectory(var name: String, val parent: Option[HailstormDirectory] = None, children: MSet[HailstormPath] = MSet.empty) extends HailstormPath {

    def loadPersistedFiles(): Unit =
      if (name == "") {
        implicit val timeout = Timeout(30 seconds)
        val f = (HailstormFrontendFuse.storageManager ? HailstormStorageManager.ListFiles("")).mapTo[HailstormStorageManager.FilesList]
          Await.ready(f, Duration.Inf)
          f.value match {
            case Some(Success(HailstormStorageManager.FilesList(ls))) =>
              for(l <- ls) {
                val file :: dirs = l.split('/').toList.reverse
                var lastParent = this
                for(dir <- dirs.reverse) {
                  lastParent = new HailstormDirectory(dir, Some(this))
                  add(lastParent)
                }
                lastParent.add(file)
              }
            case _ => log.warn("Failed to retrieve persistent files list!")
          }
      }

    override def getattr(stat: FileStat): Unit = {
      stat.st_mode.set(FileStat.S_IFDIR | 0x1FF) // 0777 in octal
      stat.st_uid.set(getContext.uid.get)
      stat.st_gid.set(getContext.gid.get)
    }

    override def delete(): Unit = withLock {
      children foreach (_.delete())
      parent foreach (_ rem this)
    }

    def rem(path: HailstormPath): Unit = withLock {
      children -= path
    }

    override def rename(newName: String): Unit = withLock {
      this.name = newName
    }

    override def find(path: List[String]): Option[HailstormPath] = withLock {
      path match {
        case Nil => Some(this)
        case x :: xs => children.find(_.name == x).flatMap(_.find(xs))
      }
    }

    def readdir(buf: Pointer, filler: FuseFillDir): Unit = withLock {
      children filter (_.name(0) != '.') foreach { c => filler.apply(buf, c.name, null, 0) }
    }

    def add(name: String): Unit = withLock {
      val me = this
      children += new HailstormFile(name, Some(me))
    }

    def add(path: HailstormPath): Unit = withLock {
      children += path
    }

  }

  class HailstormFile(var name: String, val parent: Option[HailstormDirectory]) extends HailstormPath {

    implicit val timeout = Timeout(30 seconds)

    var fileHandle: HailstormStorageManager.FileOpened = null

    override def getattr(stat: FileStat): Unit = {
      stat.st_mode.set(FileStat.S_IFREG | 0x1FF) // 0777 in octal
      stat.st_uid.set(getContext.uid.get)
      stat.st_gid.set(getContext.gid.get)

      if (fileHandle != null) {
        val f = fileHandle.ref ? HailstormFileHandle.GetAttr
        Await.ready(f, Duration.Inf)
        f.value match {
          case Some(Success(HailstormFileHandle.FileAttr(size))) => stat.st_size.set(size)
          case _ => -ErrorCodes.EIO
        }
      }
    }

    override def delete(): Unit = {
      // First get ourselves out of the metadata
      withLock {
        parent foreach (_ rem this)
      }

      // Then delete the contents in Hailstorm
      var success = false
      do {
        val f = (HailstormFrontendFuse.storageManager ? HailstormStorageManager.DeleteFile(path.mkString(HailstormPath.SEPARATOR.toString))).mapTo[HailstormStorageManager.FileDeleted.type]
        Await.ready(f, Duration.Inf)
        f.value match {
          case Some(Success(HailstormStorageManager.FileDeleted)) => success = true; fileHandle = null
          case _ => // do nothing, we loop and retry
        }
      } while (!success)
    }

    override def rename(newName: String): Unit = {
      // First rename the contents in Hailstorm
      var success = false
      do {
        val f = (HailstormFrontendFuse.storageManager ? HailstormStorageManager.RenameFile(path.mkString(HailstormPath.SEPARATOR.toString), (path.dropRight(1) ::: List(newName)).mkString(HailstormPath.SEPARATOR.toString))).mapTo[HailstormStorageManager.FileRenamed.type]
        Await.ready(f, Duration.Inf)
        f.value match {
          case Some(Success(HailstormStorageManager.FileRenamed)) => success = true
          case _ => // do nothing, we loop and retry
        }
      } while (!success)

      // Then change the name in the metadata
      withLock {
        this.name = newName
      }
    }

    override def find(path: List[String]): Option[HailstormPath] = withLock {
      Some(this)
    }

    def read(buf: Pointer, @size_t size: Long, @off_t offset: Long, fi: FuseFileInfo): Int = {
      if (fileHandle != null || open() == 0) {
        val f = fileHandle.ref ? HailstormFileHandle.Read(buf, size, offset)
        Await.ready(f, Duration.Inf)
        f.value match {
          case Some(Success(HailstormFileHandle.ReadAck(sizeRead))) => sizeRead.toInt
          case _ => -ErrorCodes.EIO
        }
      } else {
        -ErrorCodes.ECONNRESET
      }
    }

    def write(buf: Pointer, @size_t size: Long, @off_t offset: Long, fi: FuseFileInfo): Int = {
      if (fileHandle != null || open() == 0) {
        val f = fileHandle.ref ? HailstormFileHandle.Write(buf, size, offset)
        Await.ready(f, Duration.Inf)
        f.value match {
          case Some(Success(HailstormFileHandle.WriteAck(sizeWritten))) => sizeWritten.toInt
          case _ => -ErrorCodes.EIO
        }
      } else {
        -ErrorCodes.ECONNRESET
      }
    }

    def close(): Int =
      if (fileHandle != null) {
        val f = HailstormFrontendFuse.storageManager ? HailstormStorageManager.CloseFile(path.mkString(HailstormPath.SEPARATOR.toString))
        Await.ready(f, Duration.Inf)
        f.value match {
          case Some(Success(HailstormStorageManager.FileClosed)) => fileHandle = null; 0
          case _ => -ErrorCodes.ECONNRESET
        }
      } else {
        0 // do nothing
      }

    def truncate(@size_t size: Long): Int = {
      if (fileHandle != null || open() == 0) {
        val f = fileHandle.ref ? HailstormFileHandle.Trunc(size)
        Await.ready(f, Duration.Inf)
        f.value match {
          case Some(Success(HailstormFileHandle.TruncAck)) => 0
          case _ => -ErrorCodes.EIO
        }
      } else {
        -ErrorCodes.ECONNRESET
      }
    }

    def open(): Int = {
      val f = (HailstormFrontendFuse.storageManager ? HailstormStorageManager.OpenFile(path.mkString(HailstormPath.SEPARATOR.toString))).mapTo[HailstormStorageManager.FileOpened]
      Await.ready(f, Duration.Inf)
      f.value match {
        case Some(Success(handle)) => fileHandle = handle; 0
        case _ => -ErrorCodes.ECONNRESET
      }
    }

    def flush(): Int = {
      if (fileHandle != null || open() == 0) {
        val f = fileHandle.ref ? HailstormFileHandle.Flush
        Await.ready(f, Duration.Inf)
        f.value match {
          case Some(Success(HailstormFileHandle.FlushAck)) => 0
          case _ => -ErrorCodes.EIO
        }
      } else {
        -ErrorCodes.ECONNRESET
      }
    }
  }

  class HailstormLink(var name: String, val target: String, val parent: Option[HailstormDirectory]) extends HailstormPath {

    override def getattr(stat: FileStat): Unit = {
      stat.st_mode.set(FileStat.S_IFLNK | 0x1FF) // 0777 in octal
      stat.st_uid.set(getContext.uid.get)
      stat.st_gid.set(getContext.gid.get)
      stat.st_nlink.set(1)
      stat.st_size.set(target.length)
    }

    override def delete(): Unit = {
      withLock {
        parent foreach (_ rem this)
      }
    }

    override def rename(newName: String): Unit =
      withLock {
        this.name = newName
      }

    override def find(path: List[String]): Option[HailstormPath] = withLock {
      Some(this)
    }
  }

  object HailstormPath {
    val SEPARATOR = '/'
  }

}