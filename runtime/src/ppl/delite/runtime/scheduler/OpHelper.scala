package ppl.delite.runtime.scheduler

import ppl.delite.runtime.graph.ops._
import ppl.delite.runtime.codegen.kernels.scala._
import ppl.delite.runtime.graph.DeliteTaskGraph
import ppl.delite.runtime.graph.targets.Targets

/**
 * Author: Kevin J. Brown
 * Date: 12/18/10
 * Time: 6:04 PM
 * 
 * Pervasive Parallelism Laboratory (PPL)
 * Stanford University
 */

object OpHelper {

  def expand(op: DeliteOP, numChunks: Int, graph: DeliteTaskGraph) = op match {
    case map: OP_Map => Map_SMP_Array_Header_Generator.makeHeader(map, graph)
    case reduce: OP_Reduce => Reduce_SMP_Array_Header_Generator.makeHeader(reduce, numChunks, graph)
    case mapReduce: OP_MapReduce => MapReduce_SMP_Array_Header_Generator.makeHeader(mapReduce, numChunks, graph)
    case zip: OP_Zip => Zip_SMP_Array_Header_Generator.makeHeader(zip, graph)
    case foreach: OP_Foreach => Foreach_SMP_Array_Header_Generator.makeHeader(foreach, graph)
    case single: OP_Single => error("OP Single cannot be expanded")
    case other => error("OP type not recognized: " + other.getClass.getSimpleName)
  }

  def split(op: DeliteOP, chunkIdx: Int, numChunks: Int, kernelPath: String) = op match {
    case map: OP_Map => Map_SMP_Array_Generator.makeChunk(map, chunkIdx, numChunks, kernelPath)
    case reduce: OP_Reduce => Reduce_SMP_Array_Generator.makeChunk(reduce, chunkIdx, numChunks, kernelPath)
    case zip: OP_Zip => Zip_SMP_Array_Generator.makeChunk(zip, chunkIdx, numChunks, kernelPath)
    case mapReduce: OP_MapReduce => MapReduce_SMP_Array_Generator.makeChunk(mapReduce, chunkIdx, numChunks, kernelPath)
    case foreach: OP_Foreach => Foreach_SMP_Array_Generator.makeChunk(foreach, chunkIdx, numChunks, kernelPath)
    case single: OP_Single => error("OP Single cannot be split")
    case other => error("OP type not recognized: " + other.getClass.getSimpleName)
  }

  def splitGPU(op: DeliteOP) = op match {
    case map: OP_Map => map.setKernelName(map.function); map
    case reduce: OP_Reduce => reduce.setKernelName(reduce.function); reduce
    case zip: OP_Zip => zip.setKernelName(zip.function); zip
    case mapReduce: OP_MapReduce => mapReduce.setKernelName(mapReduce.function); mapReduce
    case foreach: OP_Foreach => foreach.setKernelName(foreach.function); foreach
    case single: OP_Single => error("OP Single cannot be split")
    case other => error("OP type not recognized: " + other.getClass.getSimpleName)
  }
}
