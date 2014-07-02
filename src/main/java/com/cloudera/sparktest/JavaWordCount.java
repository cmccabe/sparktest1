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

package com.cloudera.sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

public final class JavaWordCount {
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }
    SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = sc.textFile(args[0]);
    JavaRDD<Integer> wordsPerLine = lines.map(new Function<String, Integer>() {
      public Integer call(String s) { return s.split(" ").length; }
    });
    Integer total = wordsPerLine.fold(Integer.valueOf(0),
        new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    });
    System.out.println("total words = " + total);
  }
}
