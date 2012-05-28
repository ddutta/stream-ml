package com.streamml.sgdspray
import backtype.storm.tuple.{ Fields, Tuple, Values }
import backtype.storm.topology.OutputFieldsDeclarer

class SGDSlaveBolt extends SGDBolt {

  //need to fill this with random values later - using 1 for testing
  var weights = Array.fill(SGDSlaveBoltConsts.maxdim){ 1.0 }
  var latestexample:List[String] = Nil
  
  override def declareOutputFields(declarer: OutputFieldsDeclarer) = {
    declarer.declareStream(SGDStreamIDs.SLAVE_MASTER, new Fields("timestamp", "transactionID", "sid", "n", "features"))
    declarer.declareStream(SGDStreamIDs.SLAVE_STATE, new Fields("timestamp", "transactionID", "n", "weights"))
  }
  
  /* 
   * This method will be called whenever a tuple is received by the bolt. 
   */
  def execute(t: Tuple) = t.getSourceStreamId match {
    // This is the place where we process messages from the master
    case SGDStreamIDs.MASTER_SLAVE => { 
      //val example = t.getString(5)
      val example = t.getString(4).split(" ").toList
      val tid = t.getInteger(1)
      latestexample = example
      val label = example.head
      val featurelist = example.tail
      var dotProduct = 0.0
      featurelist.foreach { x =>
        //count = count+1
        val pair = x.split(":").toArray
        val fid = pair(0).toInt
        val fval = pair(1).toDouble
        dotProduct = dotProduct + weights(fid) * fval
      }
      // Update the weights
      featurelist.foreach { x =>
        //count = count+1
        val pair = x.split(":").toArray
        val fid = pair(0).toInt
        val fval = pair(1).toDouble
        weights(fid) = weights(fid) + 2*SGDSlaveBoltConsts.learningrate*(dotProduct-label.toDouble)*fval
      }
      
      // TODO: We need to optionally sync with the state bolt and send it over to the state bolt
      val weightlist = weights.elements.toList
      (using anchor t).toStream(SGDStreamIDs.SLAVE_STATE).emit("timestamp", tid :java.lang.Integer, SGDSlaveBoltConsts.maxdim :java.lang.Integer, weightlist.mkString(" "))
      t.ack
    }
    case SGDStreamIDs.STATE_SLAVE => {
      // TODO handle the average done by the state bolt. 
      // Replace the weights by the ones you get from the state bolt.
      val tid = t.getIntegerByField("transactionID")
      val newweight = t.getString(3).split(" ").toArray
      println(" in slavebolt receiving incoming weight, dimension = "+newweight.length)
      for(i <- 0 until weights.length){
        weights(i)=newweight(i).toDouble
      }
      t.ack
    }
    case _ => error("Invalid stream ID received by SGDMasterBolt")
  }
}