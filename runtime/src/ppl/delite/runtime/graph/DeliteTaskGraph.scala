package ppl.delite.runtime.graph

import java.io.File
import _root_.scala.util.parsing.json.JSON
import ops._
import collection.mutable.HashMap
import targets._

object DeliteTaskGraph {

  //implicit conversion to strings
  implicit def anyToString(obj: Any): String = obj.toString

  def apply(degFile: File) = {
    val contents = scala.io.Source.fromFile(degFile).mkString
    JSON.parseFull(contents) match {
      case Some(json) => buildFromParsedJSON(json)
      case None => throw new RuntimeException("Couldn't parse the DEG file")
    }

  }

  def buildFromParsedJSON(json: Any) = {
    implicit val graph = new DeliteTaskGraph
    json match {
      case degm: Map[Any,Any] => parseDEGMap(degm)
      case err@_ => mapNotFound(err)
    }
    graph
  }

  def parseDEGMap(degm: Map[Any, Any])(implicit graph: DeliteTaskGraph) {
    val deg = getFieldMap(degm, "DEG")
    graph._version = getFieldDouble(deg, "version")
    graph._kernelPath = getFieldString(deg, "kernelpath")
    parseOps(getFieldList(deg, "ops"))
  }

  def parseOps(ops: List[Any])(implicit graph: DeliteTaskGraph) {
    for(_op <- ops) {
      val op = _op.asInstanceOf[Map[Any, Any]]
      val opType = getFieldString(op, "type")
      opType match {
        case "SingleTask" => processCommon(op, "OP_Single")
        case "MapReduce" => processCommon(op, "OP_MapReduce")
        case "Map" => processCommon(op, "OP_Map")
        case "Reduce" => processCommon(op, "OP_Reduce")
        case "ZipWith" => processCommon(op, "OP_Zip")
        case "Foreach" => processCommon(op, "OP_Foreach")
        case "Conditional" => processIfThenElseTask(op)
        case "WhileLoop" => processWhileTask(op)
        case "IndexedLoop" => processForTask(op)
        case "LoopIndex" => processForIndex(op)
        case "Arguments" => processArgumentsTask(op)
        case "EOP" => processEOPTask(op)
        case err@_ => unsupportedType(err)
      }
    }
  }

  def getFieldString(map: Map[Any, Any], field:String): String = {
    map.get(field) match {
      case Some(field) => field
      case None => fieldNotFound(field, map)
    }
  }

  def getFieldDouble(map: Map[Any, Any], field: String): Double = {
    map.get(field) match {
      case Some(field) => java.lang.Double.parseDouble(field)
      case None => fieldNotFound(field, map)
    }
  }

  def getFieldList(map: Map[Any, Any], field: String): List[Any] = {
    map.get(field) match {
      case Some(field) => field match {
        case list: List[Any] => list
        case err@_ => listNotFound(err)
      }
      case None => fieldNotFound(field, map)
    }
  }

  def getFieldMap(map: Map[Any, Any], field: String): Map[Any,Any] = {
    map.get(field) match {
      case Some(field) => field match {
        case map: Map[Any,Any] => map
        case err@_ => mapNotFound(err)
      }
      case None => fieldNotFound(field, map)
    }
  }

  def getOp(ops: scala.collection.mutable.Map[String, DeliteOP], op: String) = {
    ops.get(op) match {
      case Some(op) => op
      case None => opNotFound(op)
    }
  }

  def processCommon(op: Map[Any, Any], opType: String)(implicit graph: DeliteTaskGraph) {
    val id = getFieldString(op, "kernelId")

    val targets = getFieldList(op, "supportedTargets")
    val types = getFieldMap(op, "return-types")
    var resultMap = Map[Targets.Value,String]()
    for (target <- Targets.values) {
      if (targets.contains(target.toString)) {
        resultMap += target -> getFieldString(types, target.toString)
      }
    }

    val newop = opType match {
      case "OP_Single" => new OP_Single(id, "kernel_"+id, resultMap)
      case "OP_MapReduce" => new OP_MapReduce(id, "kernel_"+id, resultMap)
      case "OP_Map" => new OP_Map(id, "kernel_"+id, resultMap)
      case "OP_Reduce" => new OP_Reduce(id, "kernel_"+id, resultMap)
      case "OP_Zip" => new OP_Zip(id, "kernel_"+id, resultMap)
      case "OP_Foreach" => new OP_Foreach(id, "kernel_"+id, resultMap)
      case other => error("OP Type not recognized: " + other)
    }

    //handle inputs
    val inputs = getFieldList(op, "inputs")
    for(i <- inputs.reverse) {
      val input = getOp(graph._ops, i)
      newop.addInput(input)
      newop.addDependency(input)
      input.addConsumer(newop)
    }

    //handle anti dependencies
    val antiDeps = getFieldList(op, "antiDeps")
    for(a <- antiDeps) {
      val antiDep = getOp(graph._ops, a)
      newop.addDependency(antiDep)
      antiDep.addConsumer(newop)
    }

    //handle control dependencies
    val controlDeps = getFieldList(op, "controlDeps")
    for(c <- controlDeps) {
      val controlDep = getOp(graph._ops, c)
      newop.addDependency(controlDep)
      controlDep.addConsumer(newop)
    }

    //add new op to graph list of ops
    graph._ops += id -> newop

    //process target metadata
    if (resultMap.contains(Targets.Cuda)) processCudaMetadata(op, newop)

    //last op will be result op
    graph._result = newop

  }

  def processIfThenElseTask(op: Map[Any, Any])(implicit graph: DeliteTaskGraph) {
    // get id
    val id = getFieldString(op,"outputId")
    //get predicate, then, and else kernels
    val predOp = getOp(graph._ops, getFieldString(op, "conditionKernelId"))
    val thenIds = getFieldList(op, "thenKernelIds")
    val elseIds = getFieldList(op, "elseKernelIds")
    val beginOp = new OP_BeginCondition(id+"b", predOp)
    val beginElseOp = new OP_BeginElse(id+"e")
    val endOp = new OP_EndCondition(id)

    //list of all dependencies of the if block
    val ifDeps = getFieldList(op, "controlDeps") ++ getFieldList(op, "antiDeps")

    //beginning depends on all exterior dependencies
    for (depId <- ifDeps) {
      val dep = getOp(graph._ops, depId)
      beginOp.addDependency(dep)
      dep.addConsumer(beginOp)
    }

    //beginning depends on predicate resolution
    beginOp.addDependency(predOp)
    predOp.addConsumer(beginOp)

    //all thenOps depend on beginning, and begin-else depends on them
    for (thenId <- thenIds) {
      val thenOp = getOp(graph._ops, thenId)
      thenOp.addDependency(beginOp)
      beginOp.addConsumer(thenOp)
      thenOp.addConsumer(beginElseOp)
      beginElseOp.addDependency(thenOp)
    }

    //all elseOps depend on begin-else, ending depends on them
    for (elseId <- elseIds) {
      val elseOp = getOp(graph._ops, elseId)
      elseOp.addDependency(beginElseOp)
      beginElseOp.addConsumer(elseOp)
      elseOp.addConsumer(endOp)
      endOp.addDependency(elseOp)
    }

    //add a direct link between begin/end else because else block can be empty
    endOp.addDependency(beginElseOp)
    beginElseOp.addConsumer(endOp)

    //add to graph
    graph._ops += id+"b" -> beginOp
    graph._ops += id+"e" -> beginElseOp
    graph._ops += id -> endOp //endOp will be found by future ops when searching by graph id

    graph._result = endOp
  }

  def processWhileTask(op: Map[Any, Any])(implicit graph: DeliteTaskGraph) {
    // get id
    val id = getFieldString(op,"outputId")
    //get predicate and body kernels
    val predIds = getFieldList(op, "condIds")
    val bodyIds = getFieldList(op, "bodyIds")

    var predOps: List[DeliteOP] = Nil
    var bodyOps: List[DeliteOP] = Nil
    var contOps: List[DeliteOP] = Nil

    //copy the predicate in order to run it after the body as well
    for (predId <- predIds) {
      val predOp = getOp(graph._ops, predId)
      assert(predOp.isInstanceOf[OP_Single]) //TODO: should this be relaxed?
      val contOp = predOp.asInstanceOf[OP_Single].duplicate(predOp.id+"p")
      contOp.consumerList = Nil
      graph._ops += contOp.id -> contOp

      predOps ::= predOp
      contOps ::= contOp
    }
    for (bodyId <- bodyIds) bodyOps ::= getOp(graph._ops, bodyId)

    //fix up the internal dependencies of the copied predicate ops
    for (contOp <- contOps) {
      for (dep <- contOp.getDependencies) {
        val idx = predOps findIndexOf { _ == dep }
        if (idx != -1) {
          val newOp = contOps(idx)
          contOp.replaceDependency(dep, newOp)
          newOp.addConsumer(contOp)
        }
      }
      for (input <- contOp.getInputs) {
        val idx = predOps findIndexOf { _ == input }
        if (idx != -1) contOp.replaceInput(input, contOps(idx))
      }
    }

    val beginOp = new OP_BeginWhile(id+"b", predOps.head)
    val endOp = new OP_EndWhile(id, contOps.head)

    //list of all dependencies of the while block
    val whileDeps = getFieldList(op, "controlDeps") ++ getFieldList(op, "antiDeps")

    //beginning depends on all exterior dependencies
    for (depId <- whileDeps) {
      val dep = getOp(graph._ops, depId)
      beginOp.addDependency(dep)
      dep.addConsumer(beginOp)
    }

    //beginning depends on initial predicate resolution
    beginOp.addDependency(predOps.head)
    predOps.head.addConsumer(beginOp)

    //all bodyOps depend on beginning, predOps depend on them
    for (bodyOp <- bodyOps) {
      bodyOp.addDependency(beginOp)
      beginOp.addConsumer(bodyOp)
      for (contOp <- contOps) {
        contOp.addDependency(bodyOp)
        bodyOp.addConsumer(contOp)
      }
    }

    //ending depends on final contOp
    contOps.head.addConsumer(endOp)
    endOp.addDependency(contOps.head)

    //add to graph
    graph._ops += id+"b" -> beginOp
    graph._ops += id -> endOp //endOp will be found by future ops when searching by graph id

    graph._result = endOp
  }

  def processForIndex(op: Map[Any, Any])(implicit graph: DeliteTaskGraph) {
    //get id
    val id = getFieldString(op, "outputId")
    val indexOp = new OP_ForIndex(id)

    graph._ops += id -> indexOp
    graph._result = indexOp
  }

  def processForTask(op: Map[Any, Any])(implicit graph: DeliteTaskGraph) {
    // get id
    val id = getFieldString(op,"outputId")
    //get predicate and body kernels
    val indexOp = getOp(graph._ops, getFieldString(op, "indexId")).asInstanceOf[OP_ForIndex]
    val bodyIds = getFieldList(op, "bodyIds")

    val (startIdxOp, startIdxValue) = if (getFieldString(op, "startType") == "symbol") {
      (getOp(graph._ops, getFieldString(op, "startValue")), "")
    } else {
      (null, getFieldString(op, "startValue"))
    }
    val (endIdxOp, endIdxValue) = if (getFieldString(op, "endType") == "symbol") {
      (getOp(graph._ops, getFieldString(op, "endValue")), "")
    } else {
      (null, getFieldString(op, "endValue"))
    }

    val beginOp = new OP_BeginFor(id+"b", indexOp, startIdxOp, startIdxValue, endIdxOp, endIdxValue)
    val endOp = new OP_EndFor(id, indexOp)

    //list of all dependencies of the for block
    val forDeps = getFieldList(op, "controlDeps") ++ getFieldList(op, "antiDeps")

    //initial loop index depends on all exterior dependencies
    for (depId <- forDeps) {
      val dep = getOp(graph._ops, depId)
      indexOp.addDependency(dep)
      dep.addConsumer(indexOp)
    }

    //beginning depends on initial loop index
    beginOp.addDependency(indexOp)
    indexOp.addConsumer(beginOp)

    //all bodyOps depend on beginning, ending depends on them
    for (bodyId <- bodyIds) {
      val bodyOp = getOp(graph._ops, bodyId)
      bodyOp.addDependency(beginOp)
      beginOp.addConsumer(bodyOp)
      bodyOp.addConsumer(endOp)
      endOp.addDependency(bodyOp)
    }

    //add to graph
    graph._ops += id+"b" -> beginOp
    graph._ops += id -> endOp //endOp will be found by future ops when searching by graph id

    graph._result = endOp
  }

  /**
   * Add the Arguments op to the task graph
   * This op feeds all application ops that consume command line arguments
   * By definition it has no dependencies
   */
  def processArgumentsTask(op: Map[Any, Any])(implicit graph: DeliteTaskGraph) {
    val kernelId = getFieldString(op, "kernelId")
    Arguments.id = kernelId
    graph._ops += kernelId -> Arguments
    graph._result = Arguments
  }

  /**
   * Add the EOP op to the task graph
   * This op follows the application result
   */
  def processEOPTask(op: Map[Any, Any])(implicit graph: DeliteTaskGraph) {
    val result = graph._result
    EOP.addDependency(result) //EOP depends on "result" of application
    result.addConsumer(EOP)
    graph._result = EOP //set EOP as "result" for scheduler
  }

  /**
   * Extract the required Cuda metadata from the DEG
   */
  def processCudaMetadata(op: Map[Any, Any], newop: DeliteOP)(implicit graph: DeliteTaskGraph) {
    val metadataAll = getFieldMap(op, "metadata")
    val metadataMap = getFieldMap(metadataAll, "cuda")
    val cudaMetadata = newop.cudaMetadata

    for (input <- getFieldList(metadataMap, "gpuInputs").reverse) { //input list
      val value = (input.asInstanceOf[Map[String,Any]].values.head).asInstanceOf[List[Any]]
      val data = cudaMetadata.newInput
      data.resultType = value.head
      data.func = value.tail.head
    }

    val tempSyms = new HashMap[String,DeliteOP]
    for (temp <- getFieldList(metadataMap, "gpuTemps").reverse) {
      val key = (temp.asInstanceOf[Map[String,Any]].keys.head)
      val tempOp = new OP_Single(key, null, null)
      tempSyms += key -> tempOp
      cudaMetadata.tempOps ::= tempOp
    }

    def getOpLike(sym: String) = graph._ops.getOrElse(sym, tempSyms(sym))

    for (temp <- getFieldList(metadataMap, "gpuTemps").reverse) { //temporaries list
      val value = (temp.asInstanceOf[Map[String,Any]].values.head).asInstanceOf[List[Any]]
      val data = cudaMetadata.newTemp
      data.resultType = value.head
      data.func = value.tail.head
      for (sym <- value.tail.tail.head.asInstanceOf[List[String]].reverse) {
        data.inputs ::= getOpLike(sym)
      }
    }

    //output allocation
    val outList = getFieldMap(metadataMap, "gpuOutput").values.head.asInstanceOf[List[Any]]
    cudaMetadata.outputAlloc.resultType = outList.head
    cudaMetadata.outputAlloc.func = outList.tail.head
    for (sym <- outList.tail.tail.head.asInstanceOf[List[String]].reverse) {
      cudaMetadata.outputAlloc.inputs ::= getOpLike(sym)
    }
    //output copy
    cudaMetadata.outputSet.func = outList.tail.tail.tail.head

    def fill(field: String) {
      val list = getFieldList(metadataMap, field)
      val data = cudaMetadata(field)
      data.func = list.head
      for (sym <- list.tail.head.asInstanceOf[List[String]].reverse) data.inputs ::= getOpLike(sym)
    }

    fill("gpuBlockSizeX") //threads/block - x
    fill("gpuBlockSizeY") //threads/block - y
    fill("gpuBlockSizeZ") //threads/block - z
    fill("gpuDimSizeX") //blocks in grid - x
    fill("gpuDimSizeY") //blocks in grid - y

  }

  def unsupportedType(err:String) = throw new RuntimeException("Unsupported Op Type found: " + err)
  def fieldNotFound(field: String, obj: Any) = throw new RuntimeException("Expecting field [" + field + "], found: " + obj )
  def mapNotFound(err:Any) = throw new RuntimeException("Expecting a Map object, found: " + err)
  def listNotFound(err:Any) = throw new RuntimeException("Expecting a List object, found: " + err)
  def opNotFound(op:String) = throw new RuntimeException("Couldn't find following op: " + op)
}


class DeliteTaskGraph {

  val _ops = new HashMap[String, DeliteOP]
  var _result: DeliteOP = _

  var _version = 0.0
  var _kernelPath = ""

  def result : DeliteOP = _result
  def version: Double = _version
  def kernelPath: String = _kernelPath
  def ids: Iterable[String] = _ops.keys
  def ops: Iterable[DeliteOP] = _ops.values

}