package ppl.dsl.optiml

import datastruct.scala._
import java.io.PrintWriter
import ppl.delite.framework.{DeliteApplication, DSLType}
import scala.virtualization.lms.util.OverloadHack
import ppl.delite.framework.ops.DeliteOpsExp
import scala.virtualization.lms.common.{Variables, BaseExp, Base}
import scala.virtualization.lms.internal.{CGenBase, GenerationFailedException, CudaGenBase, ScalaGenBase}

trait TrainingSetOps extends DSLType with Variables with OverloadHack {
  this: OptiML =>

  object TrainingSet {
    def apply[A:Manifest,B:Manifest](xs: Rep[Matrix[A]], labels: Rep[Labels[B]]) = trainingset_obj_fromMat(xs, labels)
  }

  implicit def repTrainingSetToTrainingSetOps[A:Manifest,B:Manifest](x: Rep[TrainingSet[A,B]]) = new trainingSetOpsCls(x)
  implicit def varToTrainingSetOps[A:Manifest,B:Manifest](x: Var[TrainingSet[A,B]]): trainingSetOpsCls[A,B]

  class trainingSetOpsCls[A:Manifest,B:Manifest](x: Rep[TrainingSet[A,B]]) {
    def t = trainingset_transposed(x)
    def labels = trainingset_labels(x)
    def numSamples = x.numRows
    def numFeatures = x.numCols

    def update(i: Rep[Int], j: Rep[Int], y: Rep[A]) = throw new UnsupportedOperationException("Training sets are immutable")
    def update(i: Rep[Int], y: Rep[Vector[A]]) = throw new UnsupportedOperationException("Training sets are immutable")
    def updateRow(row: Rep[Int], y: Rep[Vector[A]]) = throw new UnsupportedOperationException("Training sets are immutable")
    def +=(y: Rep[Vector[A]]) = throw new UnsupportedOperationException("Training sets are immutable")
    def ++=(y: Rep[Matrix[A]]) = throw new UnsupportedOperationException("Training sets are immutable")
    def insertRow(pos: Rep[Int], y: Rep[Vector[A]]) = throw new UnsupportedOperationException("Training sets are immutable")
    def insertAllRows(pos: Rep[Int], y: Rep[Matrix[A]]) = throw new UnsupportedOperationException("Training sets are immutable")
    def insertCol(pos: Rep[Int], y: Rep[Vector[A]]) = throw new UnsupportedOperationException("Training sets are immutable")
    def insertAllCols(pos: Rep[Int], y: Rep[Matrix[A]]) = throw new UnsupportedOperationException("Training sets are immutable")
    def removeRow(pos: Rep[Int]) = throw new UnsupportedOperationException("Training sets are immutable")
    def removeRows(pos: Rep[Int], len: Rep[Int]) = throw new UnsupportedOperationException("Training sets are immutable")
    def removeCol(pos: Rep[Int]) = throw new UnsupportedOperationException("Training sets are immutable")
    def removeCols(pos: Rep[Int], len: Rep[Int]) = throw new UnsupportedOperationException("Training sets are immutable")
  }
      
  // object defs
  def trainingset_obj_fromMat[A:Manifest,B:Manifest](xs: Rep[Matrix[A]], labels: Rep[Labels[B]]): Rep[TrainingSet[A,B]]
  
  // class defs
  def trainingset_transposed[A:Manifest,B:Manifest](x: Rep[TrainingSet[A,B]]): Rep[TrainingSet[A,B]]
  def trainingset_labels[A:Manifest,B:Manifest](x: Rep[TrainingSet[A,B]]): Rep[Labels[B]]
}

trait TrainingSetOpsExp extends TrainingSetOps with BaseExp { this: DeliteOpsExp with OptiMLExp =>
  implicit def varToTrainingSetOps[A:Manifest,B:Manifest](x: Var[TrainingSet[A,B]]) = new trainingSetOpsCls(readVar(x))
  
  // implemented via method on real data structure
  case class TrainingSetObjectFromMat[A:Manifest,B:Manifest](xs: Exp[Matrix[A]], labels: Exp[Labels[B]]) extends Def[TrainingSet[A,B]] {
     val mM = manifest[TrainingSetImpl[A,B]]
  }
  case class TrainingSetTransposed[A:Manifest,B:Manifest](x: Exp[TrainingSet[A,B]]) extends Def[TrainingSet[A,B]]
  case class TrainingSetLabels[A:Manifest,B:Manifest](x: Exp[TrainingSet[A,B]]) extends Def[Labels[B]]

  def trainingset_obj_fromMat[A:Manifest,B:Manifest](xs: Exp[Matrix[A]], labels: Exp[Labels[B]]) = reflectEffect(TrainingSetObjectFromMat(xs, labels))
  def trainingset_transposed[A:Manifest,B:Manifest](x: Exp[TrainingSet[A,B]]) = TrainingSetTransposed(x)
  def trainingset_labels[A:Manifest,B:Manifest](x: Exp[TrainingSet[A,B]]) = TrainingSetLabels(x)
}

trait ScalaGenTrainingSetOps extends ScalaGenBase {
  val IR: TrainingSetOpsExp
  import IR._

  override def emitNode(sym: Sym[_], rhs: Def[_])(implicit stream: PrintWriter) = rhs match {
    // these are the ops that call through to the underlying real data structure
    case t@TrainingSetObjectFromMat(xs, labels) => emitValDef(sym, "new " + remap(t.mM) + "(" + quote(xs) + "," + quote(labels) + ")")
    case TrainingSetTransposed(x) => emitValDef(sym, quote(x) + ".transposed")
    case TrainingSetLabels(x) => emitValDef(sym, quote(x) + ".labels")
    case _ => super.emitNode(sym, rhs)
  }
}

trait CudaGenTrainingSetOps extends CudaGenBase {
  val IR: TrainingSetOpsExp
  import IR._

  override def emitNode(sym: Sym[_], rhs: Def[_])(implicit stream: PrintWriter) = rhs match {

    case TrainingSetObjectFromMat(xs, labels) => throw new GenerationFailedException("CudaGen: TrainingSet Cannot be generated from GPU")
    case TrainingSetTransposed(x) => emitValDef(sym, "(*"+quote(x) + ".transposed)")
    //case TrainingSetLabels(x) => emitValDef(sym, quote(x) + ".labels")
    case _ => super.emitNode(sym, rhs)
  }
}

trait CGenTrainingSetOps extends CGenBase {
  val IR: TrainingSetOpsExp
  import IR._

  override def emitNode(sym: Sym[_], rhs: Def[_])(implicit stream: PrintWriter) = rhs match {

    case t@TrainingSetObjectFromMat(xs, labels) => emitValDef(sym, "new " + remap(t.mM) + "(" + quote(xs) + "," + quote(labels) + ")")
    case TrainingSetTransposed(x) => emitValDef(sym, quote(x) + ".transposed")
    case TrainingSetLabels(x) => emitValDef(sym, quote(x) + ".labels")
    case _ => super.emitNode(sym, rhs)
  }
}
