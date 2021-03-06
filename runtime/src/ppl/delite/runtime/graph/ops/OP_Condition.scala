package ppl.delite.runtime.graph.ops

import ppl.delite.runtime.graph.DeliteTaskGraph
import ppl.delite.runtime.graph.targets.Targets

/**
 *
 */

class OP_Condition(val id: String, resultType: Map[Targets.Value, String],
                   val predicateGraph: DeliteTaskGraph, val predicateValue: String,
                   val thenGraph: DeliteTaskGraph, val thenValue: String,
                   val elseGraph: DeliteTaskGraph, val elseValue: String)
  extends OP_Control {

  def supportsTarget(target: Targets.Value) = resultType.contains(target)

  def outputType(target: Targets.Value) = resultType(target)
  override def outputType: String = resultType(Targets.Scala)

  def nestedGraphs = Seq(predicateGraph, thenGraph, elseGraph)

  def isReturner(idx: Int) = {
    if (thenGraph.result != null && !thenGraph.result.isInstanceOf[OP_Input])
      (thenGraph.result.scheduledResource == idx)
    else if (elseGraph.result != null && !elseGraph.result.isInstanceOf[OP_Input])
      (elseGraph.result.scheduledResource == idx)
    else true //should only be 1 in this case
  }

  def returner(indices: Seq[Int]) = {
    if (thenGraph.result != null && !thenGraph.result.isInstanceOf[OP_Input])
      thenGraph.result.scheduledResource
    else if (elseGraph.result != null && !elseGraph.result.isInstanceOf[OP_Input])
      elseGraph.result.scheduledResource
    else indices(0)
  }

  /**
   * creates a Condition chunk for each requested resource and destroys the original
   */
  def makeChunks(indices: Seq[Int], graph: DeliteTaskGraph) = {
    var returnOp: OP_Condition = null
    val returnerIdx = returner(indices)
    val chunks =
      for (idx <- indices) yield {
        val resultMap = if (idx == returnerIdx) resultType else Targets.unitTypes
        val r = new OP_Condition(id+"_"+idx, resultMap, predicateGraph, predicateValue,
        thenGraph, thenValue, elseGraph, elseValue)
        r.dependencyList = dependencyList
        r.inputList = inputList
        r.consumerList = consumerList
        r.inputSyms = inputSyms
        r.cudaMetadata = cudaMetadata
        for (dep <- getDependencies) dep.addConsumer(r)
        for (c <- getConsumers) c.addDependency(r)
        if (idx == returnerIdx) returnOp = r

        //add special consumer ops
        if (predicateValue == "") predicateGraph.schedule(idx).add(new GetterOp(id+"p_"+idx, idx, Seq(predicateGraph.result), Seq(predicateGraph.result))) //get predicate result on all chunks
        if (resultMap(Targets.Scala) != "Unit") { //returns result and isReturner
          if (thenValue == "") thenGraph.schedule(idx).add(new GetterOp(id+"t_"+idx, idx, Seq(thenGraph.result), Seq(thenGraph.result))) //get then result on returner chunk
          if (elseValue == "") elseGraph.schedule(idx).add(new GetterOp(id+"e_"+idx, idx, Seq(elseGraph.result), Seq(elseGraph.result))) //get else result on returner chunk
        }

        r
      }

    graph.replaceOp(this, returnOp)
    chunks
  }

}
