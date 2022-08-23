package rule_query_translator.test

import org.scalacheck.*
import org.scalacheck.Prop.{propBoolean, passed}
import rule_query_translator.Translation.*
import rule_query_translator.Datalog.{Class => DClass, getProgramClass, programShow}
import rule_query_translator.SPARQL.{Class => SClass, getScriptClass, combineInsertDatas, scriptShow, similar}
import cats.*
import cats.implicits.*

object TranslationTest extends Properties("Translation") {
    import Prop.forAll

    property("cycleCorrectnessDatalog") = forAll(DatalogTest.genProgram) { p =>
        if p.isEmpty then
            passed
        else
            getProgramClass(p) match {
                case Left(e) => throw e
                case Right(DClass.LDP) => 
                    {
                        val t1 = translateToSparql(p, false)
                        t1.map(getScriptClass(_)) match {
                            case Left(e) => throw e
                            case Right(SClass.ISS(_)) => passed
                            case Right(SClass.DSS) =>
                                val t2 = t1.flatMap(translate(_, false))
                                (f"toSPARQL = ${t1.map(_.show)}; back = ${t2.map(_.show)}; original = ${p.show}") |: (t2 match {
                                    case Right(pn) => pn.toSet == p.toSet
                                    case Left(e) => throw e
                                })
                        }
                    }
                case Right(DClass.LADP(op)) =>
                    {
                        val t1 = translateToSparql(op, false)
                        t1.map(getScriptClass(_)) match {
                            case Left(e) => throw e
                            case Right(SClass.ISS(_)) => passed
                            case Right(SClass.DSS) =>
                                val t2 = t1.flatMap(translate(_, false))
                                (f"toSPARQL = ${t1.map(_.show)}; back = ${t2.map(_.show)}; original = ${p.show}") |: (t2 match {
                                    case Right(pn) => pn.toSet == p.toSet
                                    case Left(e) => throw e
                                })
                        }
                    }
                case Right(DClass.RDP(_)) => passed
            }
    }

    property("cycleCorrectnessSPARQL") = forAll(SPARQLTest.genScript) { s =>
        if s.isEmpty then
            passed
        else
            getScriptClass(s) match {
                case SClass.DSS => 
                    {
                        val t1 = translateToDatalog(s, false)
                        t1.flatMap(getProgramClass(_)) match {
                            case Left(e) => throw e
                            case Right(DClass.LDP) =>
                                val t2 = t1.flatMap(translateToSparql(_, false))
                                (f"Case LDP: toDatalog = ${t1.map(_.show)}; back = ${t2.map(_.show)}; original = ${s.show}") |: (t2 match {
                                    case Right(sn) => similar(sn, s)
                                    case Left(e) => throw e
                                })
                            case Right(DClass.RDP(_)) => passed
                            case Right(DClass.LADP(_)) =>
                                val t2 = t1.flatMap(translateToSparql(_, false))
                                (f"Case LADP: toDatalog = ${t1.map(_.show)}; back = ${t2.map(_.show)}; original = ${s.show}") |: (t2 match {
                                    case Right(sn) => similar(sn, s)
                                    case Left(e) => throw e
                                })
                        }
                    }
                case SClass.ISS(_) => passed
            }
    }
}