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
  
  /* 
   * In order to declare a stream, we need to override this method, and precisely define the stream
   * We need to make this a little automated. 
   * For now, we say that this bolt outputs on the MASTER_SLAVE with the following fields. 
   * We cannot emit a tuple that has a different format. 
   */
  override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
    declarer.declareStream(SGDStreamIDs.STATE_SLAVE, new Fields("timestamp", "transactionID", "n", "sum"))
  }
  
  private val transactions = new HashMap[Int, SGDStateTransaction]
  
  /* 
   * This method will be called whenever a tuple is received by the bolt. 
   */
  def execute(t: Tuple) = t.getSourceStreamId match {
    case SGDStreamIDs.SLAVE_STATE => { 
      val tid = t.getIntegerByField("transactionID")
      val sum = t.getDoubleByField("sum")
      var trans = transactions.get(tid)
      println(" in statebolt sum = "+sum + "trans class id is "+trans)
      if (trans !=None) {
        println("in state transaction previously seen id is "+trans + " nSeen = " +trans.get.nSeen + " n is " +n)
        val tsum  = trans.get.sum + sum 
        // update the sum and check if all the subtuples have been seen 
        if (trans.get.nSeen == n-1) {
          // time to send the sum
          (using anchor t).toStream(SGDStreamIDs.STATE_SLAVE).emit("timestamp", tid:java.lang.Integer, 
              n:java.lang.Integer, tsum:java.lang.Double) 
          t.ack
          // remove the transaction
          transactions.remove(tid)
        } else {
          // just add update 
          trans.get.sum += sum
          transactions.updated(tid,trans.get)
        }
      } else {
        // new transaction seen 
        println("in state new transaction seen")
        transactions.put(tid, new SGDStateTransaction(tid, sum, 1))
      }
      // Emit to the particular stream, else we would say 
      // using anchor t emit (...)
      //(using anchor t).toStream(SGDStreamIDs.STATE_SLAVE).emit("timestamp", "transactionID", n:java.lang.Integer, sum:java.lang.Double) 
      //t ack
    }
    case _ => error("Invalid stream ID received by SGDMasterBolt")
  }
}