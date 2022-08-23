package rule_query_translator

/** Provides ADTs and methods for dealing with the SPARQL language.
  *
  * The main datatype is [[rule_query_translator.SPARQL.Script]], which captures a SPARQL Script consisting of multiple [[rule_query_translator.SPARQL.Query]] (an arbitrary number of INSERT (DATA) queries and exactly one SELECT query).
  */

package object SPARQL {
    
    import cats.*
    import cats.implicits.*
    import org.eclipse.rdf4j.sparqlbuilder.core.QueryElement
    import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries
    import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery
    import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern
    import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder
    import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns
    import org.eclipse.rdf4j.model.impl.SimpleIRI
    import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf
    import org.eclipse.rdf4j.sparqlbuilder.core.{Variable => Rdf4jVariable}
    import org.eclipse.rdf4j.query.parser.ParsedQuery
    import org.eclipse.rdf4j.query.parser.ParsedUpdate
    import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor
    import org.eclipse.rdf4j.query.algebra.StatementPattern
    import org.eclipse.rdf4j.query.algebra.Var
    import org.eclipse.rdf4j.query.algebra.ProjectionElemList
    import org.eclipse.rdf4j.query.algebra.ProjectionElem
    import org.eclipse.rdf4j.query.algebra.Modify
    import org.eclipse.rdf4j.query.parser.sparql.{SPARQLParser => Rdf4jSPARQLParser}
    import org.eclipse.rdf4j.query.BindingSet
    import cats.effect.{IO, Resource}
    import org.eclipse.rdf4j.repository.RepositoryConnection
    import org.eclipse.rdf4j.repository.Repository
    import org.eclipse.rdf4j.rio.RDFFormat
    import scala.jdk.CollectionConverters.*
    import cats.data.EitherT
    import RDF.addDataToRepository
    import scala.util.Try
    import monocle.syntax.all.*
    import java.util.ArrayList
    import Exceptions.*
    import java.nio.file.Path
    
    /*
    Definition of a SPARQL-Script:
    */

    type Prefix = Tuple2[String, String]
    
    implicit val prefixShow: Show[Prefix] =
        Show.show(t => "PREFIX " + t._1 + ": <" + t._2 + ">")
    
    type Script = Vector[Query]
    
    implicit val scriptShow: Show[Script] =
        Show.show(_.map(_.show).mkString("\n\n"))
    
    enum Query:
        case Insert(triples: Vector[Triple], triplesBlock: Vector[Triple])
        case InsertData(triples: Vector[Triple])
        case Select(variables: Vector[Term], triplesBlock: Vector[Triple])
    import Query.*
    
    implicit val queryShow: Show[Query] =
        Show.show({
            case q: Insert => "INSERT \n{\n\t" + q.triples.map(_.show).mkString(".\n\t") + ".\n}\nWHERE\n{\n\t" + q.triplesBlock.map(_.show).mkString(".\n\t") + ".\n}"
            case q: InsertData => "INSERT DATA \n{\n\t" + q.triples.map(_.show).mkString(".\n\t") + ".\n}"
            case q: Select => "SELECT " + q.variables.map(_.show).mkString(",") + "\nWHERE\n{\n\t" + q.triplesBlock.map(_.show).mkString(".\n\t") + ".\n}"
        })

    enum QueryType:
        case Insert
        case InsertData
        case Select
    
    type Triple = Tuple3[Term, Term, Term]
    
    implicit val tripleShow: Show[Triple] =
        Show.show(t => t._1.show + " " + t._2.show + " " + t._3.show)
    
    enum Term:
        case Constant(n: Symbol)
        case Variable(n: Symbol)
    import Term.*
    
    implicit val termShow: Show[Term] =
        Show.show({case t: Constant => "<" + t.n + ">"
                   case t: Variable => "?" + t.n})
    
    type Symbol = String

    enum Class:
        case DSS
        case ISS(predicates: Set[Term])

    /*
    Helpers:
    */

    /** Returns the (last) select query contained in the script (if there is one). */
    private[SPARQL] def getSelectQuery(s: Script): Option[Query] =
        s.findLast(query => query match {
            case Select(_, _) => true
            case Insert(_,_) => false
            case InsertData(_) => false
        })

    /** Returns the insert (data) queries contained in the script (if there is at least one). */
    private[SPARQL] def getInsertQueries(s: Script): Option[Script] =
        val filtered = s.filter(query => query match {
            case Insert(_, _) => true
            case InsertData(_) => true 
            case Select(_,_) => false
        })
        if filtered.isEmpty then None else Some(filtered)

    /** Combines all INSERT DATA statements in the script to one. */
    def combineInsertDatas(s: Script): Script =
        def h(a: Tuple2[Option[Query], Script], query: Query) =
            val (insertData, acc) = a
            query match {
                case i @ (_: Query.Insert | _: Query.Select) => (insertData, acc :+ i)
                case i @ InsertData(triples) => insertData match {
                    case None => (Option(i),acc)
                    case Some(InsertData(triplesOld)) => (Option(InsertData(triplesOld ++ triples)), acc)
                }
        }
        val (insertData, rest) = s.foldLeft((None, Vector()))(h)
        insertData.map(Vector(_)).getOrElse(Vector()) ++ rest

    /** Returns the predicates contained in the INSERT clauses of INSERT queries. */
    private[SPARQL] def insertQueryInsertClausePredicates(s: Script): Set[Term] =
        s.foldLeft(Set())({
            case (s, Insert(triples, _)) => s ++ triples.map(_._2).toSet
            case (s, InsertData(_)) => s
            case (s, Select(_, _)) => s
        })
    
    /** Returns the predicates contained in the WHERE clauses of INSERT queries. */
    private[SPARQL] def insertQueryWhereClausePredicates(s: Script): Set[Term] =
        s.foldLeft(Set())({
            case (s, Insert(_, triplesBlock)) => s ++ triplesBlock.map(_._2).toSet
            case (s, InsertData(_)) => s
            case (s, Select(_, _)) => s
        })
    
    /** Returns whether the two given scripts are "similar".
      * 
      * Two scripts are similar if they contain the same queries, but possibly combined into fewer queries or split out into more queries (this is possible for INSERT DATA and INSERT queries).
      * Also the order of the queries is not checked.
      */
    def similar(x: Script, y: Script): Boolean =
        def contained(z: Query) = z match {
            case InsertData(triplesX) => triplesX.forall((triple: Triple) => y.exists(_ match {
                case InsertData(triplesY) => triplesY.contains(triple)
                case _ => false
            }))
            case Insert(triplesX, triplesBlockX) => triplesX.forall((triple: Triple) => y.exists(_ match {
                case Insert(triplesY, triplesBlockY) => triplesY.contains(triple) && triplesBlockX == triplesBlockY
                case _ => false
            }))
            case sx @ Select(_, _) => y.exists(_ match {
                case  sy @ Select(_, _) => sx == sy
                case _ => false
            })
        }
        x.forall(contained(_))

    /*
    Parser for SPARQL Script:
    */
    
    /** Parse a Script from a given String.
      * 
      * @param s The String to parse from: The input needs to consist of valid PREFIX Statements (optional), INSERT DATA queries (optional), INSERT queries (optional) and a SELECT query (mandatory), devided by blank lines and in that order.
      * @return Either a [[Exceptions.ParsingException]] describing the error parsing or a SPARQL script.
      */
    def parseString(s: String): Either[ParsingException, Script] =
        for
            parsed <- SPARQLParser.execute(s + "\n")
            queries <- parsed match {
                case (prefixes, insertDatas, inserts, select) => {
                    val prefs = prefixes.map(_.show).mkString("\n")
                    (
                        insertDatas.map(prefs + "\n" + _),
                        inserts.map(prefs + "\n" + _),
                        prefs + "\n" + select
                    ).asRight[ParsingException]
                }
            }
            insertDatas <- queries._1.map(parseToInsertData).sequence.map(_.map(_.asInstanceOf[Query]))
            inserts <- queries._2.map(parseToInsert).sequence.map(_.map(_.asInstanceOf[Query]))
            select <- parseToSelect(queries._3)
        yield
            insertDatas ++ inserts :+ select
    
    private[SPARQL] object SPARQLParser {
        import cats.parse.Parser.Expectation.*
        import cats.parse.Parser.Expectation
        import cats.parse.{Parser => P, Parser0}
        import cats.parse.Rfc5234.*
        import cats.data.NonEmptyList

        type ParsedScript = (Vector[Prefix], Vector[String], Vector[String], String)
        
        def execute(input: String): Either[ParsingException, ParsedScript] = {
            script.parseAll(input) match {
              case Right(script) => script.asRight[ParsingException]
              case Left(e) => ParsingException(getErrorDescription(e, input), input, "SPARQLScript").asLeft[ParsedScript]
            }
        }
        
        def getErrorDescription(e: cats.parse.Parser.Error, input: String): String =
            def getExpectationDescription(ex: Expectation): String = ex match {
              case OneOfStr(_, strs) => f"Expected one of ${strs.map(""""""" + _ + """"""").mkString(", ")}"
              case InRange(_, lower, upper) => f"Expected a character between $lower and $upper"
              case StartOfString(_) => f"Expected start of the string"
              case EndOfString(_, _) => f"Expected end of the string"
              case Length(_, expected, actual) => f"Expected string of length $expected but got of length $actual"
              case ExpectedFailureAt(_, matched) => f"Expected failure at $matched"
              case Fail(_) => f"Unknown failure"
              case FailWith(_, message) => message
              case WithContext(context, expect) => f"In the context of $context, " + getExpectationDescription(expect)
            }
            f"Error parsing at ${'"'}${input.slice(e._1, e._1 + 7)}${'"'}: " + e._2.map(getExpectationDescription).toList.mkString("; ") + "; The input needs to consist of valid PREFIX Statements (optional), INSERT DATA queries (optional), INSERT queries (optional) and a SELECT query (mandatory), devided by blank lines and in that order."
        
        def whitespace: P[Char] = P.charIn(" \t")
        def newline: P[String] = (P.string("\n") | P.string("\r") | P.string("\n\r")).string
        def emptyLine: P[String] = (whitespaces0.with1 ~ newline).map({case (_, nl) => nl})
        def nonEmptyLine: P[String] = (whitespaces0.soft.with1 ~ vchar ~ (vchar | whitespace).rep0 ~ newline).map({ case (((ws, c), rest), nl) => ((c +: rest) :+ nl).mkString })
        def nonEmptyLines: P[String] = nonEmptyLine.rep.map(_.toList.mkString)
        def whitespaces0: Parser0[Unit] = whitespace.rep0.void
        def string: P[String] = alpha.rep.string
        
        def keyword(s: String): P[String] = P.string(s).surroundedBy(whitespaces0).map(_ => s)
        
        def script: P[ParsedScript] = {((P.start ~ emptyLine.void.rep0) ~ ((prefix ~ emptyLine) | emptyLine).backtrack.rep0 ~ emptyLine.rep0 ~ ((insertData ~ emptyLine) | emptyLine).backtrack.rep0 ~ emptyLine.rep0 ~ ((insert ~ emptyLine) | emptyLine).backtrack.rep0 ~ emptyLine.rep0).with1 ~ select ~ (emptyLine.void.backtrack.rep0 ~ whitespaces0 ~ P.end)}.map(_ match {
          case ((((((((_ , prefixes), _), insertDatas), _), inserts), _), select), _) => (prefixes.filter({case x@(a,b) => true; case _ => false}).map(_.asInstanceOf[(Prefix,String)]._1).toVector, insertDatas.filter({case x@(a,b) => true; case _ => false}).map(_.asInstanceOf[(String, String)]._1).toVector, inserts.filter({case x@(a,b) => true; case _ => false}).map(_.asInstanceOf[(String, String)]._1).toVector, select)
        })
        
        def prefix: P[Prefix] = {keyword("PREFIX").void *> identifier ~ whitespaces0 ~ iri}.map((t: Tuple2[Tuple2[String,Unit],String]) => (t._1._1,t._2))
        def identifier: P[String] = string <* P.char(':')
        def iriChar: P[Char] = alpha | P.charIn(":+.-/@[]#") | digit
        def iri: P[String] = (P.char('<').void *> iriChar.rep <* P.char('>').void).map((cs: NonEmptyList[Char]) => cs.toList.mkString)
        
        def insertData: P[String] = {keyword("INSERT DATA") ~ newline ~ nonEmptyLines}.map((t: ((String, String), String)) => t._1._1 + t._1._2 + t._2)
        
        def insert: P[String] = {keyword("INSERT") ~ newline ~ nonEmptyLines}.map((t: ((String, String), String)) => t._1._1 + t._1._2 + t._2)
        
        def select: P[String] = {keyword("SELECT") ~ nonEmptyLines}.map((t: (String, String)) => t._1 + " " + t._2)
    }
    
    private[SPARQL] val rdf4jSparqlParser = new Rdf4jSPARQLParser()

    /** Parses a given String to a [[Query.Select]].
      * 
      * Important: If the query supplied to this method is not a *simple* SELECT query, then the behavior is undefined!
      * 
      * @param s The String containing one *simple* SELECT query.
      * @return Either a [[Exceptions.ParsingException]] describing the error that occured translating or a SELECT query.
      */
    private[SPARQL] def parseToSelect(s: String): Either[ParsingException, Query] =
        val parsed = Try(rdf4jSparqlParser.parseQuery(s, null)).fold(
            e => Left(ParsingException("Parsing the SELECT query with the RDF4J parser was unsuccessful.", s, "SPARQLScript", Some(e))),
            v => Right(v)
        )
        parsed.flatMap(selectFromRdf4j(_).fold(
            l => Left(ParsingException("Translating SELECT query to internal SPARQL from RDF4j was unsuccessfull.", s, "SPARQLScript", Some(l))),
            r => r.asRight[ParsingException]
        ))

    
    /** Parses a given String to a [[Query.Insert]].
      * 
      * Important: If the query supplied to this method is not a *simple* INSERT query, then the behavior is undefined!
      * 
      * @param s The String containing one *simple* INSERT query.
      * @return Either a [[Exceptions.ParsingException]] describing the error that occured translating or a INSERT query.
      */
    private[SPARQL] def parseToInsert(s: String): Either[ParsingException, Query] =
        val parsed = Try(rdf4jSparqlParser.parseUpdate(s, null)).fold(
            e => Left(ParsingException("Parsing the INSERT query with the RDF4J parser was unsuccessful.", s, "SPARQLScript", Some(e))),
            v => Right(v)
        )
        parsed.flatMap(insertFromRdf4j(_).fold(
            l => Left(ParsingException("Translating INSERT query to internal SPARQL from RDF4j was unsuccessfull.", s, "SPARQLScript", Some(l))),
            r => r.asRight[ParsingException]
        ))

    /** Parses a given String to a [[Query.InsertData]].
      * 
      * Important: If the query supplied to this method is not a *simple* INSERT DATA query, then the behavior is undefined!
      * 
      * @param s The String containing one *simple* INSERT DATA query.
      * @return Either a [[Exceptions.ParsingException]] describing the error that occured translating or a INSERT DATA query.
      */
    private[SPARQL] def parseToInsertData(s: String): Either[ParsingException, Query] =
        val dummyWhere = """
        WHERE
            {
                <http://dummy.com/subject> <http://dummy.com/predicate> <http://dummy.com/object> 
            }
        """
        val transformedInput = s.replace(" DATA","") + "\n" + dummyWhere
        val parsed = Try(rdf4jSparqlParser.parseUpdate(transformedInput, null)).fold(
            e => Left(ParsingException("Parsing the INSERT DATA query with the RDF4J parser was unsuccessful.", s, "SPARQLScript", Some(e))),
            v => Right(v)
        )
        val result = parsed.flatMap(insertFromRdf4j(_).fold(
            l => Left(ParsingException("Translating INSERT DATA query to internal SPARQL from RDF4j was unsuccessfull.", s, "SPARQLScript", Some(l))),
            r => r.asRight[ParsingException]
        ))
        result.map((q: Query) => Query.InsertData(q.asInstanceOf[Query.Insert].triples).asInstanceOf[Query])

    /*
    Definition of translation from the RDF4J libarary:
    */
    
    /** A visitor ([[org.eclipse.rdf4j.query.algebra.QueryModelVisitor]]) to construct a [[Query.Select]] from a [[org.eclipse.rdf4j.query.parser.ParsedQuery]].
      * 
      * ## Important
      * If the query this visitor is used on is not a *simple* SELECT query, then the behavior is undefined!
      */ 
    private[SPARQL] class SelectTranslationVisitor extends AbstractQueryModelVisitor[java.lang.Exception]:
        private var query: Query.Select = Select(Vector(), Vector())
        private var error: Option[TranslationException] = None

        override def meet(node: StatementPattern) =
            if (error.isEmpty) {
                val vars = node.getVarList.asScala.toVector.map(fromRdf4j).sequence
                vars.fold({
                    s => error = Some(s)
                }, {
                    v => {
                        val triple: Triple = (v(0), v(1), v(2))
                        query = query.focus(_.triplesBlock).modify(_.appended(triple))
                    }
                })
            }

        override def meet(node: ProjectionElemList) =
            if (error.isEmpty) {
                val vars = node.getElements.asScala.toVector.map(fromRdf4j).sequence
                vars.fold({
                    s => error = Some(s)
                }, {
                    v => {
                        query = query.focus(_.variables).replace(v)
                    }
                })
            }

        def getQuery: Either[TranslationException, Query] =
            if error.isDefined then error.get.asLeft[Query] else
                if query.triplesBlock.length > 0 then query.asInstanceOf[Query].asRight[TranslationException] else
                    Left(TranslationException("Visitor could not construct Query.", "{Not avaiable}", "RDF4J", "SPARQLScript"))

    /** A visitor ([[org.eclipse.rdf4j.query.algebra.QueryModelVisitor]]) to construct a [[Query.Insert]] from a [[org.eclipse.rdf4j.query.parser.ParsedUpdate]].
      * 
      * ## Important
      * If the query this visitor is used on is not a *simple* INSERT query, then the behavior is undefined!
      */ 
    private[SPARQL] class InsertTranslationVisitor extends AbstractQueryModelVisitor[java.lang.Exception]:
        private var query: Option[Query.Insert] = None
        private var error: Option[TranslationException] = None

        override def meet(node: Modify) =
            if (error.isEmpty) {
                val insertVisitor = new TripleTranslationVisitor()
                val whereVisitor = new TripleTranslationVisitor()
                node.getInsertExpr.visit(insertVisitor)
                node.getWhereExpr.visit(whereVisitor)
                val inserts = insertVisitor.getTriples
                val wheres = whereVisitor.getTriples
                (inserts, wheres).tupled match {
                    case Left(s) => error = Some(s)
                    case Right((triples, triplesBlock)) =>
                        query = Some(Query.Insert(triples, triplesBlock))
                }
            }

        def getQuery: Either[TranslationException, Query] =
            if error.isDefined then error.get.asLeft[Query] else
                if query.isDefined then query.get.asInstanceOf[Query].asRight[TranslationException] else
                    Left(TranslationException("Visitor could not construct Query.", "{Not avaiable}", "RDF4J", "SPARQLScript"))

    /** A visitor ([[org.eclipse.rdf4j.query.algebra.QueryModelVisitor]]) to construct a Vector of [[Triple]] from a [[org.eclipse.rdf4j.query.algebra.TupleExpr]].
      * 
      * ## Important
      * If the expression this visitor is used on is not a *simple* tuple expression (as part of a *simple* insert query), then the behavior is undefined!
      */ 
    private[SPARQL] class TripleTranslationVisitor extends AbstractQueryModelVisitor[java.lang.Exception]:
        private var triples: Vector[Triple] = Vector()
        private var error: Option[TranslationException] = None

        override def meet(node: StatementPattern) =
            if (error.isEmpty) {
                val vars = node.getVarList.asScala.toVector.map(fromRdf4j).sequence
                vars.fold({
                    s => error = Some(s)
                }, {
                    v => triples = triples :+ (v(0), v(1), v(2))
                })
            }

        def getTriples: Either[TranslationException, Vector[Triple]] =
            if error.isDefined then error.get.asLeft[Vector[Triple]] else
                if triples.nonEmpty then triples.asRight[TranslationException] else Left(TranslationException("No Triple extracted from StatementPattern", "{Not avaiable}", "RDF4J", "SPARQLScript"))
    
    /** Translates a given [[org.eclipse.rdf4j.query.parser.ParsedQuery]] to a [[Query.Select]].
      * 
      * Important: If the query supplied to this method is not a *simple* SELECT query, then the behavior is undefined!
      * 
      * @param q The ParsedQuery from the RDF4J library.
      * @return Either a [[Exceptions.TranslationException]] describing the error that occured translating or a SELECT query.
      */
    def selectFromRdf4j(q: ParsedQuery): Either[TranslationException, Query] =
        val tuple = q.getTupleExpr
        val visitor = new SelectTranslationVisitor()
        tuple.visit(visitor)
        visitor.getQuery
    
    /** Translates a given [[org.eclipse.rdf4j.query.parser.ParsedUpdate]] to a [[Query.Insert]].
      * 
      * Important: If the query supplied to this method is not a *simple* INSERT query, then the behavior is undefined!
      * 
      * @param q The ParsedUpdate from the RDF4J library.
      * @return Either a [[Exceptions.TranslationException]] describing the error that occured translating or a INSERT query.
      */
    def insertFromRdf4j(q: ParsedUpdate): Either[TranslationException, Query] =
        q.getUpdateExprs.asScala.toVector match {
            case Vector() => Left(TranslationException("No UpdateExpression in ParsedUpdate.", q.toString, "RDF4J", "SPARQLScript"))
            case Vector(update) => {
                val visitor = new InsertTranslationVisitor()
                update.visit(visitor)
                visitor.getQuery
            }
            case _ => Left(TranslationException("More than one UpdateExpression in ParsedUpdate.", q.toString, "RDF4J", "SPARQLScript"))
        }

    private[SPARQL] def fromRdf4j(v: Var): Either[TranslationException, Term] =
        if (v.isConstant) {
            Term.Constant(v.getValue.stringValue).asRight[TranslationException]
        } else {
            Term.Variable(v.getName).asRight[TranslationException]
        }
    
    private[SPARQL] def fromRdf4j(pe: ProjectionElem): Either[TranslationException, Term] =
        Term.Variable(pe.getSourceName).asRight[TranslationException]
    
    /*
    Definition of translation to the RDF4J libarary:
    */
    
    /** Translates from the interal SPARQL script to the RDF4J Library.
      * 
      * @param xs A [[rule_query_translator.SPARQL.Script]]
      * @return Either a [[Exceptions.TranslationException]] describing the error translating or a vector of [[org.eclipse.rdf4j.sparqlbuilder.core.QueryElement]].
      */
    def toRdf4j(s: Script): Either[TranslationException, Vector[QueryElement]] =
        s.map(toRdf4j).sequence
    
    private[SPARQL] def toRdf4j(q: Query): Either[TranslationException, QueryElement] = q match {
        case Insert(triples, triplesBlock) => {
            val query = Queries.INSERT()
            for (t <- triples) {
                query.insert(toRdf4j(t))
            }
            for (t <- triplesBlock) {
                query.where(toRdf4j(t))
            }
            query.asRight[TranslationException]
        }
        case Select(variables, triplesBlock) => {
            val query = Queries.SELECT()
            for (v <- variables) {
                for (x <- toRdf4j(v)) yield {query.select(x)} // Does that work syntactically?
            }
            for (t <- triplesBlock) {
                query.where(toRdf4j(t))
            }
            query.asRight[TranslationException]
        }
        case InsertData(triples) => {
            val query = Queries.INSERT_DATA()
            for (t <- triples) {
                query.insertData(toRdf4j(t))
            }
            query.asRight[TranslationException]
        }
    }
    
    private[SPARQL] def toRdf4j(t: Triple): TriplePattern = t match {
        case (Variable(v1), Variable(v2), Variable(v3)) => {
            val variable1 = SparqlBuilder.`var`(v1)
            val variable2 = SparqlBuilder.`var`(v2)
            val variable3 = SparqlBuilder.`var`(v3)
            GraphPatterns.tp(variable1, variable2, variable3)
        }
        case (Variable(v1), Variable(v2), Constant(c3)) =>  {
            val variable1 = SparqlBuilder.`var`(v1)
            val variable2 = SparqlBuilder.`var`(v2)
            val constant3 = Rdf.iri(c3)
            GraphPatterns.tp(variable1, variable2, constant3)
        }
        case (Variable(v1), Constant(c2), Variable(v3)) =>  {
            val variable1 = SparqlBuilder.`var`(v1)
            val constant2 = Rdf.iri(c2)
            val variable3 = SparqlBuilder.`var`(v3)
            GraphPatterns.tp(variable1, constant2, variable3)
        }
        case (Variable(v1), Constant(c2), Constant(c3)) =>  {
            val variable1 = SparqlBuilder.`var`(v1)
            val constant2 = Rdf.iri(c2)
            val constant3 = Rdf.iri(c3)
            GraphPatterns.tp(variable1, constant2, constant3)
        }
        case (Constant(c1), Variable(v2), Variable(v3)) =>  {
            val constant1 = Rdf.iri(c1)
            val variable2 = SparqlBuilder.`var`(v2)
            val variable3 = SparqlBuilder.`var`(v3)
            GraphPatterns.tp(constant1, variable2, variable3)
        }
        case (Constant(c1), Variable(v2), Constant(c3)) =>  {
            val constant1 = Rdf.iri(c1)
            val variable2 = SparqlBuilder.`var`(v2)
            val constant3 = Rdf.iri(c3)
            GraphPatterns.tp(constant1, variable2, constant3)
        }
        case (Constant(c1), Constant(c2), Variable(v3)) =>  {
            val constant1 = Rdf.iri(c1)
            val constant2 = Rdf.iri(c2)
            val variable3 = SparqlBuilder.`var`(v3)
            GraphPatterns.tp(constant1, constant2, variable3)
        }
        case (Constant(c1), Constant(c2), Constant(c3)) =>  {
            val constant1 = Rdf.iri(c1)
            val constant2 = Rdf.iri(c2)
            val constant3 = Rdf.iri(c3)
            GraphPatterns.tp(constant1, constant2, constant3)
        }
    }
    
    private[SPARQL] def toRdf4j(t: Term): Either[TranslationException, Rdf4jVariable] = t match {
        case Variable(n) => SparqlBuilder.`var`(n).asRight[TranslationException]
        case Constant(n) => Left(TranslationException(f"Unexpected Constant ${n} in Translation to Variable", t.show, "SPARQLScript", "RDF4J"))
    }

    /*
    Run a SPARQL Scipt:
    */

    /** Evaluates the given [[org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery]] on the data saved in the given [[org.eclipse.rdf4j.repository.Repository]]. (No OWL or RDFS inference.)
      * 
      * @param repo The Repository to use.
      * @param query The SelectQuery to run on the data.
      * @return A vector of the values of the bindings, in order of projection.
      */
    def evaluate(repo: Repository, query: SelectQuery): IO[Vector[Vector[String]]] =
            val connection = Resource.fromAutoCloseable(IO(repo.getConnection()))
            connection.use {
                case conn => {
                    val tupleQuery = conn.prepareTupleQuery(query.getQueryString)
                    val result = Resource.fromAutoCloseable(IO.blocking(tupleQuery.evaluate()))
                    result.use {
                        case res => {
                            val list = new ArrayList[BindingSet]()
                            res.iterator().forEachRemaining(list.add)
                            val bindings = list.asScala.toVector
                            IO.pure(
                                for
                                    bindingSet <- bindings
                                yield
                                    for
                                        binding <- bindingSet.getBindingNames().asScala.toVector
                                    yield
                                        bindingSet.getValue(binding).stringValue()
                            )
                        }
                    }
                }
        }

    /** Evaluates the given script.
      * 
      * @param script The script to evaluate, must contain exactly one select query.
      * @param path Path to an RDF file in TURTLE syntax with additional data (optional).
      * @return Either a [[Exceptions.EvaluationException]] describing the error that occured, or a vector of the values of the bindings, in order of projection.
      */
    def evaluateQuery(script: Script, path: Option[Path]): IO[Either[EvaluationException, Vector[Vector[String]]]] =
        val repository = RDF.newRepository()
        repository.use {
            case repo => {
                path.fold(
                    IO.unit
                )(
                    RDF.addRDFToRepository(_, repo, RDFFormat.TURTLE)
                ) >>
                SPARQL.evaluateQuery(
                    script,
                    repo
                ).value
            }
        }

    /** Evaluates the given script on the given [[org.eclipse.rdf4j.repository.Repository]]. (No OWL or RDFS inference.)
      * 
      * @param script The script to evaluate, must contain exactly one select query.
      * @param repo The Repository to use.
      * @return Either a [[Exceptions.EvaluationException]] describing the error that occured, or a vector of the values of the bindings, in order of projection.
      */
    private[SPARQL] def evaluateQuery(script: Script, repo: Repository): EitherT[IO, EvaluationException, Vector[Vector[String]]] =
        val optionQuery = getSelectQuery(script)
        val optionInserts = getInsertQueries(script)
        for 
            query <- EitherT.fromEither(
                        if optionQuery.isDefined then
                        Right(optionQuery.get) else
                        Left(EvaluationException("Could not get select query from given script.", script.show, "SPARQLScript"))
                    )
            queryLiteral <- EitherT.fromEither(toRdf4j(query).fold(
                l => Left(EvaluationException("Could not convert select query to query literal.", query.show, "SPARQLScript", Some(l))),
                r => Right(r)
            ))
            selectQuery <- EitherT.fromEither(
                                Try(queryLiteral.asInstanceOf[SelectQuery]).fold(
                                    e => Left(EvaluationException("Could not convert QueryElement to SelectQuery.", queryLiteral.toString, "RDF4J", Some(e))),
                                    r => Right(r)
                                )
                           )
            statements <- EitherT.fromEither(optionInserts.map(toRdf4j).sequence.fold(
                l => Left(EvaluationException("Could not convert insert (data) queries to statements.", optionInserts.show, "SPARQLScript", Some(l))),
                r => Right(r)
            ))
            _ <- if statements.isDefined then
                    EitherT(addDataToRepository(statements.get, repo).attempt.map(_.fold(
                        l => Left(EvaluationException("Could not add RDF data to RDF4J repository.", statements.map(_.map(_.getQueryString())).toString(), "RDF4J", Some(l))),
                        r => Right(r)
                    ))) else
                    EitherT.right(IO.unit)
            answer <- EitherT.right(evaluate(repo, selectQuery))
        yield
            answer

    /*        
    Script Classes:
    */

    /** Determines the (complexity) class of the given script.
      * 
      * @param script The script to analyse.
      * @return The [[Class]] of the script, possibly containing further information (for ISS, the predicates that occur in the INSERT clause and the WHERE clause).
      */
    def getScriptClass(s: Script): Class =
        val insertPredicates = insertQueryInsertClausePredicates(s)
        val wherePredicates = insertQueryWhereClausePredicates(s)
        val problematic = insertPredicates.intersect(wherePredicates)
        if problematic.isEmpty then
            Class.DSS
        else
            Class.ISS(problematic)
}