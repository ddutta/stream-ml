package com.streamml.sgdspray

import backtype.storm.tuple.{ Fields, Tuple, Values }
import backtype.storm.topology.OutputFieldsDeclarer

class SGDMasterBolt extends SGDBolt {
  
  /* 
   * In order to declare a stream, we need to override this method, and precisely define the stream
   * We need to make this a little automated. 
   * For now, we say that this bolt outputs on the MASTER_SLAVE with the following fields. 
   * We cannot emit a tuple that has a different format. 
   */
  // TODO: Das, we need to change these formats to something real. 
  override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
    declarer.declareStream(SGDStreamIDs.MASTER_SLAVE, new Fields("timestamp", "transactionID", "sid", "n", "features"))
  }
  
  /* 
   * This method will be called whenever a tuple is received by the bolt. 
   */
  def execute(t: Tuple) = t.getSourceStreamId match {
    case SGDStreamIDs.SPOUT_MASTER => { 
      // Emit to the particular stream, else we would say 
      // using anchor t emit (...)
      val example = t.getString(3).split(" ").toList
      println(example)
      val label = example.head
      val featurelist = example.tail
      val tid = t.getInteger(1)
      val count = 1
      // TODO: Do sampling and send to the appropriate slave 
      // Once we choose the slave, we need to emit the slaveid sid s
      (using anchor t).toStream(SGDStreamIDs.MASTER_SLAVE).emit("timestamp", tid :java.lang.Integer, "sid", featurelist.length:java.lang.Integer, example.mkString(" "))
      //(using anchor t).toStream(SGDStreamIDs.MASTER_SLAVE).emit("timestamp", "transactionID", "sid", "n", "features") 
      t ack
    }
    case _ => error("Invalid stream ID received by SGDMasterBolt")
  }
}