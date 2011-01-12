package ppl.delite.runtime.graph.ops

import ppl.delite.runtime.graph.targets.Targets

/**
 * Author: Kevin J. Brown
 * Date: 1/11/11
 * Time: 7:43 PM
 * 
 * Pervasive Parallelism Laboratory (PPL)
 * Stanford University
 */

abstract class OP_For extends OP_Control

class OP_BeginFor(val id: String, val index: OP_ForIndex, val startOp: DeliteOP, val startValue: String, val endOp: DeliteOP, val endValue: String) extends OP_For {
  //TODO: this dependency management is overly conservative
  def makeChunk(idx: Int): OP_Control = {
    if (idx == 0) return this
    val chunk = new OP_BeginFor(id.dropRight(1)+"_"+idx+"b", index, startOp, startValue, endOp, endValue)
    chunk.dependencyList = dependencyList
    chunk.consumerList = consumerList
    for (dep <- getDependencies) dep.addConsumer(chunk)
    for (c <- getConsumers) c.addDependency(chunk)
    chunk
  }
}

class OP_EndFor(val id: String, val index: OP_ForIndex) extends OP_For {
  def makeChunk(idx: Int): OP_Control = {
    if (idx == 0) return this
    val chunk = new OP_EndFor(id+"_"+idx, index)
    chunk.dependencyList = dependencyList
    chunk.consumerList = consumerList
    for (dep <- getDependencies) dep.addConsumer(chunk)
    for (c <- getConsumers) c.addDependency(chunk)
    chunk
  }
}

class OP_ForIndex(val id: String) extends OP_For {
  def makeChunk(idx: Int): OP_Control = {
    if (idx == 0) return this
    val chunk = new OP_ForIndex(id+"_"+idx)
    chunk.dependencyList = dependencyList
    chunk.consumerList = consumerList
    for (dep <- getDependencies) dep.addConsumer(chunk)
    for (c <- getConsumers) c.addDependency(chunk)
    chunkList ::= chunk
    chunk
  }

  //this gives us links from original op to all chunks to make graph rewrites
  //TODO: should be a better way
  var chunkList: List[OP_ForIndex] = List(this)
}
