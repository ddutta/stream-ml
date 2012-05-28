package com.streamml.sgdspray

import backtype.storm.tuple.{ Fields, Tuple, Values }
import backtype.storm.topology.OutputFieldsDeclarer
import scala.collection.mutable.HashMap

class SGDStateTransaction(var t: Int, d: Double, ns: Int){
  /* 
   * This class stores the state needed for a single transaction
   */
  var tid = t
  var sum = d
  var nSeen = ns // when nSeen = n, time to broadcast the sum.
  require (tid>0)
  require (ns>0)
}

class SGDStateBolt (n: Int) extends SGDBolt {
    val maxdim=40
  /* 
   * In order to declare a stream, we need to override this method, and precisely define the stream
   * We need to make this a little automated. 
   * For now, we say that this bolt outputs on the MASTER_SLAVE with the following fields. 
   * We cannot emit a tuple that has a different format. 
   */
  override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
    declarer.declareStream(SGDStreamIDs.STATE_SLAVE, new Fields("timestamp", "transactionID", "n", "weights"))
  }
  
  private val transactions = new HashMap[Int, SGDStateTransaction]
  var nseen = 0;
  var weights = Array.fill(maxdim){ 0.0 }
  /* 
   * This method will be called whenever a tuple is received by the bolt. 
   */
  def execute(t: Tuple) = t.getSourceStreamId match {
    case SGDStreamIDs.SLAVE_STATE => { 
      val tid = t.getIntegerByField("transactionID")
      val newweight = t.getString(3).split(" ").toArray
      nseen = nseen + 1
      println(" in statebolt incoming weight dimension = "+newweight.length)
      for(i <- 0 until weights.length){
        weights(i)=weights(i) + newweight(i).toDouble
      }

      // update the sum and check if all the subtuples have been seen 
      if (nseen == n) {
         // time to send the sum
        for(i <- 0 until weights.length){
           weights(i)=weights(i)/n
         }
         val weightlist = weights.elements.toList
         (using anchor t).toStream(SGDStreamIDs.STATE_SLAVE).emit("timestamp", tid:java.lang.Integer, 
              n:java.lang.Integer, weightlist.mkString(" ")) 
         println(" in statebolt about to send back averaged weights")
         // Reset weight vector and nseen for the next averaging process
         nseen = 0;
         weights = Array.fill(maxdim){ 0.0 }
       } 
      // Emit to the particular stream, else we would say 
      // using anchor t emit (...)
      //(using anchor t).toStream(SGDStreamIDs.STATE_SLAVE).emit("timestamp", "transactionID", n:java.lang.Integer, sum:java.lang.Double) 
      t.ack
    }
    case _ => error("Invalid stream ID received by SGDMasterBolt")
  }
}