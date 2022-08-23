package rule_query_translator.test

import cats.*
import cats.implicits.*
import org.scalacheck.*
import Gen.*
import Arbitrary.arbitrary
import rule_query_translator.SPARQL.*
import Query.*
import Term.*

object SPARQLTest {
    def genScript =
        for
            n <- Gen.choose(0,10)
            id <- Gen.listOfN(n, genInsertData).map(_.toVector)
            n2 <- Gen.choose(0,10)
            i <- Gen.listOfN(n, genInsert).map(_.toVector)
            se <- genSelect
        yield
            id ++ i :+ se
    def genSelect =
        for
            n <- Gen.choose(1,10)
            vs <- Gen.listOfN(n, genVariable).map(_.toVector)
            n2 <- Gen.choose(1,10)
            tb <- Gen.listOfN(n2, genTriple).map(_.toVector)
        yield
            Select(vs, tb)
    def genInsertData =
        for
            n <- Gen.choose(1,10)
            ts <- Gen.listOfN(n, genConstTriple).map(_.toVector)
        yield
            InsertData(ts)
    def genInsert =
        for
            n <- Gen.choose(1,10)
            ts <- Gen.listOfN(n, genTriple).map(_.toVector)
            n2 <- Gen.choose(1,10)
            tb <- Gen.listOfN(n2, genTriple).map(_.toVector)
        yield 
            Insert(ts, tb)
    def genVariable =
        for
            n <- Gen.choose(1,10)
            s <- Gen.listOfN(n, Gen.alphaChar).map(_.mkString)
        yield
            Variable("?" + s)
    def genTriple =
        for
            t1 <- genTerm
            t2 <- genConstant
            t3 <- genTerm
        yield
            (t1,t2,t3)
    def genConstTriple =
        for
            t1 <- genTerm
            t2 <- genConstant
            t3 <- genTerm
        yield
            (t1,t2,t3)
    def genTerm =
        Arbitrary.arbitrary[Boolean].flatMap({
            case true => genConstant
            case false => genVariable
        })
    def genConstant =
        for
            n <- Gen.choose(1,10)
            s <- Gen.listOfN(n, Gen.alphaChar).map(_.mkString)
        yield
            Constant("http://" + s)

}