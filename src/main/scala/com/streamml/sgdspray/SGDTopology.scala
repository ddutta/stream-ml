package com.streamml.sgdspray

import storm.scala.dsl._
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.{Fields, Tuple, Values}
import collection.mutable.{Map, HashMap}
import util.Random

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
    cluster.submitTopology("word-count", conf, builder.createTopology)
    Thread sleep 10000
    cluster.shutdown
  }
}