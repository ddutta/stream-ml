package com.streamml.sgdspray

object SGDConsts {

}

object SGDStreamIDs {
  val SPOUT_MASTER = "spm"
  val MASTER_SLAVE = "ms"
  val SLAVE_MASTER = "sm"
  val SLAVE_STATE = "slst"
  val STATE_SLAVE = "stsl"
  val SLAVE_COMBINE = "slcmb"
}

object SGDSlaveBoltConsts { 
  val maxdim = 40
  val learningrate = 0.1
}