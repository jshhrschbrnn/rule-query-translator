package rule_query_translator.test

import cats.*
import cats.implicits.*
import org.scalacheck.*
import Gen.*
import Arbitrary.arbitrary
import rule_query_translator.Datalog.*
import Statement.*
import Term.*

object DatalogTest {
    def genProgram =
        for
            n <- Gen.choose(0,10)
            p <- Gen.listOfN(n, genStatement).map(_.toVector)
            q <- genQuery
        yield
            p :+ q
    def genQuery =
        for
            n <- Gen.choose(1,10)
            ts <- Gen.listOfN(n, genVariable.map(Variable(_))).map(_.toVector)
            n2 <- Gen.choose(1,10)
            body <- Gen.listOfN(n, genLiteral).map(_.toVector)
        yield
            Rule(("Q", ts), body)

    def genStatement =
        Arbitrary.arbitrary[Boolean].flatMap({
            case true => genLiteral.map(Fact(_))
            case false =>
                for
                    head <- genLiteral
                    n <- Gen.choose(1,10)
                    body <- Gen.listOfN(n, genLiteral).map(_.toVector)
                yield
                    Rule(head, body)
        })
    def genLiteral =
        for
            s <- genConstant
            n <- Gen.choose(1,2)
            ts <- Gen.listOfN(n, genTerm).map(_.toVector)
        yield
            (s,ts)
    def genTerm =
        Arbitrary.arbitrary[Boolean].flatMap({
            case true => genConstant.map(Constant(_))
            case false => genVariable.map(Variable(_))
        })
    def genConstant =
        for
            n <- Gen.choose(1,10)
            s <- Gen.listOfN(n, Gen.alphaChar).map(_.mkString)
        yield
            "http://" + s
    def genVariable =
        for
            n <- Gen.choose(1,10)
            s <- Gen.listOfN(n, Gen.alphaChar).map(_.mkString)
        yield
            "?" + s
}
