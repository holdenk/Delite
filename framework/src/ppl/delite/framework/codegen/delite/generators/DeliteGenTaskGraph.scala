package ppl.delite.framework.codegen.delite.generators

import ppl.delite.framework.codegen.delite.DeliteCodegen
import ppl.delite.framework.ops.DeliteOpsExp
import collection.mutable.{ArrayBuffer, ListBuffer}
import java.io.{StringWriter, FileWriter, File, PrintWriter}
import ppl.delite.framework.{Util, Config}
import scala.virtualization.lms.internal.{GenerationFailedException, ScalaGenEffect, GenericCodegen}

trait DeliteGenTaskGraph extends DeliteCodegen {
  val IR: DeliteOpsExp
  import IR._

  private def vals(sym: Sym[_]) : List[Sym[_]] = sym match {
    case Def(Reify(s, effects)) => List(s.asInstanceOf[Sym[_]])
    case Def(Reflect(NewVar(v), effects)) => Nil
    case _ => List(sym)
  }

  private def vars(sym: Sym[_]) : List[Sym[_]] = sym match {
    case Def(Reflect(NewVar(v), effects)) => List(sym)
    case _ => Nil
  }

  private def mutating(kernelContext: State, sym: Sym[_]) : List[Sym[_]] =
    kernelContext flatMap {
      //case Def(Reflect(x,effects)) => if (syms(x) contains sym) List(sym) else Nil
      case Def(Mutation(x,effects)) => if (syms(x) contains sym) List(sym):List[Sym[_]] else Nil
      case _ => Nil: List[Sym[_]]
    }

  override def emitNode(sym: Sym[_], rhs: Def[_])(implicit stream: PrintWriter) : Unit = {
    assert(generators.length >= 1)

    var resultIsVar = false
    var skipEmission = false
    nestedEmission = false
    var nestedNode: TP[_] = null
    implicit var emittedNodeList = new ListBuffer[List[Sym[_]]]

    val saveInputDeps = kernelInputDeps
    val saveMutatingDeps = kernelMutatingDeps

    // we will try to generate any node that is not purely an effect node
    rhs match {
      case Reflect(s, effects) => super.emitNode(sym, rhs); return
      case Reify(s, effects) => super.emitNode(sym, rhs); return
      case DeliteOpCondition(c,t,e) => {
        emitBlock(c)
        emittedNodeList += controlDeps
        emitBlock(t)
        emittedNodeList += emittedNodes
        emitBlock(e)
        emittedNodeList += emittedNodes
        skipEmission = true
      }
      case DeliteOpIndexedLoop(s,e,i,b) => {
        emitLoopIndex(i.asInstanceOf[Sym[Int]])
        emitBlock(b)
        emittedNodeList += emittedNodes
        skipEmission = true
      }
      case DeliteOpWhileLoop(c,b) => {
        emitBlock(c)
        emittedNodeList += controlDeps
        emittedNodeList += emittedNodes
        emitBlock(b)
        emittedNodeList += emittedNodes
        skipEmission = true
      }
      case NewVar(x) => resultIsVar = true // if sym is a NewVar, we must mangle the result type
      case _ => // continue and attempt to generate kernel
    }

    kernelInputDeps = saveInputDeps
    kernelMutatingDeps = saveMutatingDeps

    // validate that generators agree on inputs (similar to schedule validation in DeliteCodegen)
    val dataDeps = ifGenAgree(g => (g.syms(rhs) ++ g.getFreeVarNode(rhs)).distinct, true)
    val inVals = dataDeps flatMap { vals(_) }
    val inVars = dataDeps flatMap { vars(_) }

    implicit val supportedTargets = new ListBuffer[String]
    implicit val returnTypes = new ListBuffer[Pair[String, String]]
    implicit val metadata = new ArrayBuffer[Pair[String, String]]

    // parameters for delite overrides
    deliteInputs = (inVals ++ inVars)
    deliteResult = Some(sym) //findDefinition(rhs) map { _.sym }

    for (gen <- generators) {
      // reset nested flag
      gen.nestedEmission = false
      val buildPath = Config.buildDir + java.io.File.separator + gen + java.io.File.separator
      val outDir = new File(buildPath); outDir.mkdirs()
      val outFile = new File(buildPath + quote(sym) + "." + gen.kernelFileExt)
      val kstream = new PrintWriter(outFile)
      val bodyString = new StringWriter()
      val bodyStream = new PrintWriter(bodyString)

      try{
        rhs match {
          case op:DeliteOp[_] => deliteKernel = true
          case _ => deliteKernel = false
        }

        //initialize
        gen.kernelInit(sym, inVals, inVars, resultIsVar)

        // emit kernel to bodyStream //TODO: must kernel body be emitted before kernel header?
        gen.emitNode(sym, rhs)(bodyStream)
        bodyStream.flush

        val resultType = if (gen.toString == "scala") {
          rhs match {
            case map: DeliteOpMap[_,_,_] => "generated.scala.DeliteOpMap[" + gen.remap(map.v.Type) + "," + gen.remap(map.func.Type) + "," + gen.remap(map.alloc.Type) + "]"
            case zip: DeliteOpZipWith[_,_,_,_] => "generated.scala.DeliteOpZipWith[" + gen.remap(zip.v._1.Type) + "," + gen.remap(zip.v._2.Type) + "," + gen.remap(zip.func.Type) + "," + gen.remap(zip.alloc.Type) +"]"
            case red: DeliteOpReduce[_] => "generated.scala.DeliteOpReduce[" + gen.remap(red.func.Type) + "]"
            case mapR: DeliteOpMapReduce[_,_,_] => "generated.scala.DeliteOpMapReduce[" + gen.remap(mapR.mV.Type) + "," + gen.remap(mapR.reduce.Type) + "]"
            case foreach: DeliteOpForeach[_,_] => "generated.scala.DeliteOpForeach[" + gen.remap(foreach.v.Type) + "]"
            case _ => gen.remap(sym.Type)
          }
        } else gen.remap(sym.Type)

        // emit kernel
        if(skipEmission == false) {
          gen.emitKernelHeader(sym, inVals, inVars, resultType, resultIsVar)(kstream)
          kstream.println(bodyString.toString)
          gen.emitKernelFooter(sym, inVals, inVars, resultType, resultIsVar)(kstream)
        }

        //record that this kernel was successfully generated
        supportedTargets += gen.toString
        if (resultIsVar) {
          returnTypes += new Pair[String,String](gen.toString,"generated.scala.Ref[" + gen.remap(sym.Type) + "]") {
            override def toString = "\"" + _1 + "\" : \"" + _2 + "\""
          }
        }
        else {
          returnTypes += new Pair[String,String](gen.toString,gen.remap(sym.Type)) {
            override def toString = "\"" + _1 + "\" : \"" + _2 + "\""
          }
        }

        //add MetaData
        if(gen.hasMetaData) {
          metadata += new Pair[String,String](gen.toString, gen.getMetaData) {
            override def toString = "\"" + _1 + "\" : " + _2
          }
        }

        kstream.close()
      }
      catch {
        case e:GenerationFailedException => // no generator found
          gen.exceptionHandler(e, outFile, kstream)
          //e.printStackTrace
        case e:Exception => throw(e)
      }
    }

    if (skipEmission == false && supportedTargets.isEmpty) {
      var msg = "Node " + quote(sym) + "[" + rhs + "] could not be generated by any code generator"
      if(nestedEmission) msg = "Failure is in nested node " + quote(nestedNode.sym) + "[" + nestedNode.rhs + "]. " + msg
      system.error(msg)
    }

    val inputs = inVals ++ inVars
    //val kernelContext = getEffectsKernel(sym, rhs)
    val kernelContext = ifGenAgree( _.getEffectsBlock(sym), true )
    val inMutating = (inputs flatMap { mutating(kernelContext, _) }).distinct

    // additional data deps: for each of my inputs, look at the kernels already generated and see if any of them
    // mutate it, and if so, add that kernel as a data-dep
    val extraDataDeps = (kernelMutatingDeps filter { case (s, mutates) => (!(inputs intersect mutates).isEmpty) }).keys
    val inControlDeps = (controlDeps ++ extraDataDeps).distinct

    // anti deps: for each of my mutating inputs, look at the kernels already generated and see if any of them
    // read it, add that kernel as an anti-dep
    val antiDeps = (kernelInputDeps filter { case (s, in) => (!(inMutating intersect in).isEmpty) }).keys.toList

    // add this kernel to global generated state
    kernelInputDeps += { sym -> inputs }
    kernelMutatingDeps += { sym -> inMutating }

    // debug
    /*
    stream.println("inputs: " + inputs)
    stream.println("mutating inputs: " + inMutating)
    stream.println("extra data deps: " + extraDataDeps)
    stream.println("control deps: " + inControlDeps)
    stream.println("anti deps:" + antiDeps)
    */

    // emit task graph node
    rhs match {
      case DeliteOpCondition(c,t,e) => emitIfThenElse(c,sym, inputs, inControlDeps, antiDeps)
      case DeliteOpIndexedLoop(s,e,i,b) => emitIndexedLoop(s,e,i, sym, inputs, inControlDeps, antiDeps)
      case DeliteOpWhileLoop(c,b) => emitWhileLoop(sym, inputs, inControlDeps, antiDeps)
      case s:DeliteOpSingleTask[_] => emitSingleTask(sym, inputs, inControlDeps, antiDeps)
      case m:DeliteOpMap[_,_,_] => emitMap(sym, inputs, inControlDeps, antiDeps)
      case r:DeliteOpReduce[_] => emitReduce(sym, inputs, inControlDeps, antiDeps)
      case a:DeliteOpMapReduce[_,_,_] => emitMapReduce(sym, inputs,inControlDeps, antiDeps)
      case z:DeliteOpZipWith[_,_,_,_] => emitZipWith(sym, inputs, inControlDeps, antiDeps)
      case f:DeliteOpForeach[_,_] => emitForeach(sym, inputs, inControlDeps, antiDeps)
      case _ => emitSingleTask(sym, inputs, inControlDeps, antiDeps) // things that are not specified as DeliteOPs, emit as SingleTask nodes
    }

    // whole program gen (for testing)
    //emitValDef(sym, "embedding.scala.gen.kernel_" + quote(sym) + "(" + inputs.map(quote(_)).mkString(",") + ")")
  }

  /**
   * @param sym         the symbol representing the kernel
   * @param inputs      a list of real kernel dependencies (formal kernel parameters)
   * @param controlDeps a list of control dependencies (must execute before this kernel)
   * @param antiDeps    a list of WAR dependencies (need to be committed in program order)
   */
  def emitSingleTask(sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]]) = {
    stream.print("{\"type\":\"SingleTask\"")
    emitExecutionOpCommon(sym, inputs, controlDeps, antiDeps)
  }

  def emitMap(sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]]) = {
    stream.print("{\"type\":\"Map\"")
    emitExecutionOpCommon(sym, inputs, controlDeps, antiDeps)
  }

  def emitReduce(sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]]) = {
    stream.print("{\"type\":\"Reduce\"")
    emitExecutionOpCommon(sym, inputs, controlDeps, antiDeps)
  }

  def emitMapReduce(sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]]) = {
    stream.print("{\"type\":\"MapReduce\"")
    emitExecutionOpCommon(sym, inputs, controlDeps, antiDeps)
  }

  def emitZipWith(sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]]) = {
    stream.print("{\"type\":\"ZipWith\"")
    emitExecutionOpCommon(sym, inputs, controlDeps, antiDeps)
  }

  def emitForeach(sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]]) = {
    stream.print("{\"type\":\"Foreach\"")
    emitExecutionOpCommon(sym, inputs, controlDeps, antiDeps)
  }

  def emitIfThenElse(cond: Exp[Boolean], sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]], emittedNodesList: ListBuffer[List[Sym[_]]]) = {
    stream.print("{\"type\":\"Conditional\",")
    stream.println("  \"outputId\" : \"" + quote(sym) + "\",")
    val bodyIds = emittedNodesList(1) ++ emittedNodesList(2)
    val controlDepsStr = makeString(controlDeps filterNot { bodyIds contains })
    val antiDepsStr = makeString(antiDeps filterNot { bodyIds contains })
    val thenS = makeString(emittedNodesList(1))
    val elseS = makeString(emittedNodesList(2))
    stream.println("  \"conditionKernelId\" : \"" + quote(cond) + "\", ")
    stream.println("  \"thenKernelIds\" : [" + thenS + "],")
    stream.println("  \"elseKernelIds\" : [" + elseS + "],")
    stream.print("  \"controlDeps\":[" + controlDepsStr + "],\n")
    stream.println("  \"antiDeps\":[" + antiDepsStr + "]")
    stream.println("},")
  }

  def emitIndexedLoop(start: Exp[Int], end: Exp[Int], i: Exp[Int], sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]], emittedNodesList: ListBuffer[List[Sym[_]]]) = {
    stream.println("{\"type\":\"IndexedLoop\",")
    stream.println("  \"outputId\" : \"" + quote(sym) + "\",")
    val controlDepsStr = makeString(controlDeps filterNot { emittedNodesList(0) contains })
    val antiDepsStr = makeString(antiDeps filterNot { emittedNodesList(0) contains })
    def getType(e: Exp[Int]) = e match {
      case c:Const[Int] => "const"
      case s:Sym[Int]   => "symbol"
    }
    stream.print("  \"startType\" : \"" + getType(start) + "\",")
    stream.println(" \"startValue\" : \"" + quote(start) + "\",")
    stream.print("  \"endType\" : \"" + getType(end) + "\",")
    stream.println(" \"endValue\" : \"" + quote(end) + "\",")
    stream.println("  \"indexId\" : \"" + quote(i) + "\",")
    val bodyS = makeString(emittedNodesList(0))
    stream.println("  \"bodyIds\" : [" + bodyS + "],")
    stream.print("  \"controlDeps\":[" + controlDepsStr + "],\n")
    stream.println("  \"antiDeps\":[" + antiDepsStr + "]")
    stream.println("},")
  }

  def emitLoopIndex(sym: Sym[Int])(implicit stream: PrintWriter) {
    stream.println("{\"type\":\"LoopIndex\",")
    stream.println("  \"outputId\" : \"" + quote(sym) + "\"")
    stream.println("},")
  }

  def emitWhileLoop(sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]], emittedNodesList: ListBuffer[List[Sym[_]]]) = {
    stream.println("{\"type\":\"WhileLoop\",")
    stream.println("  \"outputId\" : \"" + quote(sym) + "\",")
    val controlDepsStr = makeString(controlDeps filterNot { emittedNodesList(2) contains })
    val antiDepsStr = makeString(antiDeps filterNot { emittedNodesList(2) contains })
    val conds = makeString(emittedNodesList(1))
    val bodys = makeString(emittedNodesList(2))
    stream.println("  \"condIds\" : [" + conds + "],")
    stream.println("  \"bodyIds\" : [" + bodys + "],")
    stream.print("  \"controlDeps\":[" + controlDepsStr + "],\n")
    stream.println("  \"antiDeps\":[" + antiDepsStr + "]")
    stream.println("},")
  }

  def emitControlFlowOpCommon(sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]]) = {
  }

  def emitExecutionOpCommon(sym: Sym[_], inputs: List[Exp[_]], controlDeps: List[Exp[_]], antiDeps: List[Exp[_]])
                    (implicit stream: PrintWriter, supportedTgt: ListBuffer[String], returnTypes: ListBuffer[Pair[String, String]], metadata: ArrayBuffer[Pair[String,String]]) = {
    stream.print(" , \"kernelId\" : \"" + quote(sym) + "\" ")
    stream.print(" , \"supportedTargets\": [" + supportedTgt.mkString("\"","\",\"","\"") + "],\n")
    val inputsStr = if(inputs.isEmpty) "" else inputs.map(quote(_)).mkString("\"","\",\"","\"")
    stream.print("  \"inputs\":[" + inputsStr + "],\n")
    emitDepsCommon(controlDeps, antiDeps)
    val metadataStr = if (metadata.isEmpty) "" else metadata.mkString(",")
    stream.print("  \"metadata\":{" + metadataStr + "},\n")
    val returnTypesStr = if(returnTypes.isEmpty) "" else returnTypes.mkString(",")
    stream.print("  \"return-types\":{" + returnTypesStr + "}\n")
    stream.println("},")
  }

  def emitDepsCommon(controlDeps: List[Exp[_]], antiDeps: List[Exp[_]], last:Boolean = false)(implicit stream: PrintWriter) {
    stream.print("  \"controlDeps\":[" + makeString(controlDeps) + "],\n")
    stream.print("  \"antiDeps\":[" + makeString(antiDeps) + "]" + (if(last) "\n" else ",\n"))
  }

  private def makeString(list: List[Exp[_]]) = {
    if(list.isEmpty) "" else list.map(quote(_)).mkString("\"","\",\"","\"")
  }

  // more quirks
  override def quote(x: Exp[_]) = x match {
    case r:Reify[_] => quote(r.x)
    case _ => super.quote(x)
  }




  def nop = throw new RuntimeException("Not Implemented Yet")

}
