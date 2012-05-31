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
  val timerFactor = 10 // every 10s we need to sync up with the state bolt 
}