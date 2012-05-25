package com.streamml.sgdspray

import storm.scala.dsl._
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.{Fields, Tuple, Values}

abstract class SGDBolt extends StormBolt(outputFields = List("TBD")){

}
