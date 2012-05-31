/* 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 *  Copyright (c) 2012 Debojyoti Dutta, 
 *  Copyright (c) 2012 Abhimanyu Das
 * 
 */

package com.streamml.sgdspray

import storm.scala.dsl._
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.{Fields, Tuple, Values}

object SGDTopology {
  def main(args: Array[String]) = {
    val builder = new TopologyBuilder
    val numSlaves = 2

    builder.setSpout("spout", new SGDSpout, 1)
    builder.setBolt("master", new SGDMasterBolt, 1)
        .shuffleGrouping("spout", SGDStreamIDs.SPOUT_MASTER)
    val idecl = builder.setBolt("slave", new SGDSlaveBolt, 2)
    	.shuffleGrouping("master", SGDStreamIDs.MASTER_SLAVE)
        // .fieldsGrouping("master", SGDStreamIDs.MASTER_SLAVE, new Fields("sid")) 
    	//TODO: need to use sid when we do sampling
    builder.setBolt("state", new SGDStateBolt(2), 1).shuffleGrouping("slave",SGDStreamIDs.SLAVE_STATE)
    idecl.shuffleGrouping("state",SGDStreamIDs.STATE_SLAVE)

    val conf = new Config
    conf.setDebug(true)
    conf.setMaxTaskParallelism(3)

    val cluster = new LocalCluster
    cluster.submitTopology("stream-ml", conf, builder.createTopology)
    Thread sleep 10000
    cluster.shutdown
  }
}