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

object CountFromLines {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("CountFromLines"))
    val files = sc.textFile(args(0))
    // print out all files
    val fromLines = files.flatMap { f => 
      val FromLine = """From: (.*)""".r

      f match {
        case FromLine(addr) => addr
        case _ => None
      }
    }
    fromLines.foreach(fromLine => System.out.println("CFLv2: " + fromLine))
  }
}
