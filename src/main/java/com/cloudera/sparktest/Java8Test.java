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

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

public final class Java8Test {
  static int foreachCalls = 0;

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("Java8Test");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Hello", "World"));
    JavaRDD<String> rdd2 = rdd.map((x) -> x + "ish");
    List<String> strings = rdd2.collect();
    System.out.println("out = " + String.join(" ", strings));
  }
}
