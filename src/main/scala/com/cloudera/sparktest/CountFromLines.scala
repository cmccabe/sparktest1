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
import java.io.{
  BufferedOutputStream, IOException, PrintStream, Closeable => JCloseable
}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object CountFromLines {
  def getCodec : Codec = {
    val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    return codec
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("CountFromLines"))
    val files = sc.textFile(args(0))
    // print out all files
    val fromLines = files.map { file => 
      val fs = FileSystem.get(new URI(file), SparkHadoopUtil.get.newConfiguration())
      val stream = fs.open(new Path(file))
      try {
        val source = scala.io.Source.createBufferedSource(stream)(getCodec)
        source.getLines.foreach { line =>
          val FromLine = """From: (.*)""".r
          line match {
            case FromLine(addr) => addr
            case _ => None
          }
        }
      } finally {
        stream.close
      }
    }
    val count = fromLines.count()
    val curMonoTime = System.nanoTime
    val outFile = s"/user/cmccabe/runs/CountFromLines.run.$curMonoTime"
    val fs = FileSystem.get(new URI(outFile),
        SparkHadoopUtil.get.newConfiguration())
    val outStream = fs.create(new Path(outFile))
    val bufferedStream = new BufferedOutputStream(outStream)
    val printStream = new PrintStream(bufferedStream)
    try {
      printStream.println("CountFromLines.scala finished " +
          s"successfully with count = $count")
    } finally {
      printStream.close
    }
    //System.out.println(fromLines.count())
    //fromLines.foreach(fromLine => System.out.println("CFLv2: " + fromLine))
  }
}
