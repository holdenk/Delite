package ppl.delite.runtime.scheduler

import ppl.delite.runtime.graph.ops.OP_ForIndex

/**
 * Author: Kevin J. Brown
 * Date: 1/11/11
 * Time: 10:24 PM
 * 
 * Pervasive Parallelism Laboratory (PPL)
 * Stanford University
 */

object ControlHelper {

  /**
   * This method performs schedule modifications and op dependency rewrites for control flow
   * Rewrites input lists so ops that consume the index of a FOR always consume the local index
   * TODO: empty control structures should be removed
   * TODO: body ops should only depend on local control structures
   * TODO: ending should only depend on local body
   * TODO: FOR beginning should only depend on local index
   */
  //TODO: figure out how to do all this while scheduling these ops
  def rewriteSchedule(schedule: PartialSchedule) {
    for (resource <- schedule.resources) {
      val iter = resource.iterator
      while (iter.hasNext) {
        val op = iter.next
        for (in <- op.getInputs.filter(_.isInstanceOf[OP_ForIndex])) {
          val old = in.asInstanceOf[OP_ForIndex]
          val rep = old.chunkList.filter(_.scheduledResource == op.scheduledResource).head
          op.replaceInput(old, rep)
        }
      }
    }
  }
}