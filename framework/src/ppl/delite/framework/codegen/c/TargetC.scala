package ppl.delite.framework.codegen.c

import java.io.PrintWriter
import collection.mutable.ListBuffer
import ppl.delite.framework.codegen.{CodeGenerator, Target}
import ppl.delite.framework.embedded.scala.CodeGeneratorCMisc

trait TargetC extends Target {
  import intermediate._

  val name = "C"

  lazy val applicationGenerator  = new CodeGeneratorCApplication { val intermediate: TargetC.this.intermediate.type = TargetC.this.intermediate }  

  val generators = new ListBuffer[CodeGenerator{val intermediate: TargetC.this.intermediate.type}]
  //generators += new CodeGeneratorCMisc{val intermediate: TargetC.this.intermediate.type = TargetC.this.intermediate}

}