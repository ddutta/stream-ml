package com.streamml.sgdspray

import storm.scala.dsl._
import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.{Fields, Tuple, Values}
import collection.mutable.{Map, HashMap}
import util.Random
import backtype.storm.tuple.{ Fields, Tuple, Values }
import backtype.storm.topology.OutputFieldsDeclarer

class SGDSpout extends StormSpout(outputFields = List("bla")) {
  val sentences = List("the cow jumped over the moon",
                       "an apple a day keeps the doctor away",
                       "four score and seven years ago",
                       "snow white and the seven dwarfs",
                       "i am at two with nature")
  var id = 0
  val n = Random.nextInt(100000)
  val sgddata = io.Source.fromFile("./src/main/scala/com/streamml/sgdspray/heart_scale").getLines.toList 
  override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
    declarer.declareStream(SGDStreamIDs.SPOUT_MASTER, new Fields("timestamp", "transactionID", "n", "features"))
  }
  
  def nextTuple = {
    Thread sleep 1000
    id += 1
    //toStream(SGDStreamIDs.SPOUT_MASTER).emit("timestamp", id: java.lang.Integer, n: java.lang.Integer, "features")
    toStream(SGDStreamIDs.SPOUT_MASTER).emit("timestamp", id: java.lang.Integer, n: java.lang.Integer, sgddata(Random.nextInt(sgddata.length)))
  }
}