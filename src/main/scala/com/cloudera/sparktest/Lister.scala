/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sparktest

import collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.fs._
import java.net.URI
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

object Lister {
  def countFiles(fs: FileSystem, path: String) : Long = {
    var numFiles = 0L
    for (child <- fs.listStatus(new Path(path))) {
      if (child.isFile()) {
        numFiles += 1
      } else if (child.isDirectory()) {
        numFiles += countFiles(fs, child.getPath().toString())
      }
    }
    return numFiles
  }

  def addFiles(fs: FileSystem, path: String, files: ArrayBuffer[FileStatus]) {
    System.out.println("addFiles(path = " + path + ")")
    for (child <- fs.listStatus(new Path(path))) {
      if (child.isFile()) {
        files += child
      } else if (child.isDirectory()) {
        addFiles(fs, child.getPath().toString(), files)
      }
    }
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("SparkTest1"))
    val uri = new URI(args(0))
    val fs = FileSystem.get(uri, SparkHadoopUtil.get.newConfiguration())
    //val files = new ArrayBuffer[FileStatus](500000)
    System.out.println("numFiles = " + countFiles(fs, uri.getPath()))
    //for (f <- files) {
      //System.out.println("f = " + f)
    //}

    System.out.println("SOMEBODY STOP ME")
  }
}
