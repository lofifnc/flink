package output

import java.util

import org.scalatest.Matchers
import org.scalatest.exceptions.TestFailedException

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import org.scalatest.enablers.{Aggregating, Sequencing}

class OutputMatcher[OUT] extends Matchers{

  type L = List[OUT]

  val expectedOutput: ArrayBuffer[OUT] = new ArrayBuffer[OUT]()

  val refinements: ArrayBuffer[Refinement[OUT]] = new ArrayBuffer[Refinement[OUT]]()

  def addExpectedOutput(output: List[OUT]) = {
    expectedOutput ++= output
  }

  def addExpectedOutput(output: util.Collection[OUT]) = {
    expectedOutput ++= output
  }

  def all()(implicit aggregating: Aggregating[L]) = {
    refinements += new Refinement[OUT] {
      override def matches(actualOutput: List[OUT]) =
        aggregating.containsAllOf(actualOutput, expectedOutput)
    }
  }

  def only() = {
    refinements += new Refinement[OUT] {
      override def matches(actualOutput: List[OUT]) = {
        actualOutput should contain only(expectedOutput:_*)
      }
    }
  }

  def onlyInOrder() = {
    refinements += new Refinement[OUT] {
      override def matches(actualOutput: List[OUT]) =
        actualOutput should contain inOrderOnly (expectedOutput.head,
          expectedOutput.tail.head,
          expectedOutput.tail.tail:_*)
    }
  }

  def inOrder(first: Int, second: Int, rest: Int*) = {
    refinements += new Refinement[OUT] {
      override def matches(actualOutput: List[OUT]) = {
        actualOutput should contain inOrder(
          expectedOutput(first),
          expectedOutput(second),
          rest.toList.map(expectedOutput(_)):_*)
      }
    }
  }

  def check(actual: Iterable[OUT]) : Unit = {
   val actualList = actual.toList
    if (refinements.nonEmpty) {
      try {
        refinements
          .foreach(_.matches(actualList))
        println("valid")
      }catch {
        case e: TestFailedException =>
          println("failed " + e.message)
      }
    }else{
      //TODO throw exception
      println("empty refinements")
    }
  }

  def check(actual: util.Collection[OUT]) : Unit = {
    check(actual.toList)
  }

}

trait Refinement[OUT] {
  def matches(actualOutput: List[OUT])
}
