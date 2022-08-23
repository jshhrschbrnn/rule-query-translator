package rule_query_translator

/** Provides ADTs and methods for dealing with the Datalog language.
  *
  * # Overview
  * The main datatype is [[rule_query_translator.Datalog.Program]], which captures a Datalog program consisting of multiple [[rule_query_translator.Datalog.Statement]].
  * A [[rule_query_translator.Datalog.Statement]] can either be a [[rule_query_translator.Datalog.Statement.Fact]] or a [[rule_query_translator.Datalog.Statement.Rule]].
  */
package object Datalog {
    
    import org.semanticweb.rulewerk.core.model.api.{Statement => RulewerkStatement}
    import cats.*
    import cats.implicits.{catsSyntaxFlatMapIdOps => _, *}
    import cats.syntax.functor.*
    import cats.syntax.either.*
    import cats.syntax.show.*
    import org.semanticweb.rulewerk.core.model.api.{Fact => RulewerkFact}
    import org.semanticweb.rulewerk.core.model.api.Predicate
    import org.semanticweb.rulewerk.core.model.api.{Term => RulewerkTerm, Rule => RulewerkRule, Literal => RulewerkLiteral}
    import org.semanticweb.rulewerk.core.model.api.TermType
    import org.semanticweb.rulewerk.core.model.api.QueryResult
    import org.semanticweb.rulewerk.core.model.api.PositiveLiteral
    import scala.jdk.CollectionConverters.*
    import org.semanticweb.rulewerk.core.model.implementation.*
    import org.semanticweb.rulewerk.parser.RuleParser
    import org.semanticweb.rulewerk.core.reasoner.KnowledgeBase;
    import org.semanticweb.rulewerk.core.reasoner.QueryResultIterator;
    import org.semanticweb.rulewerk.reasoner.vlog.VLogReasoner;
    import util.Try
    import util.Success
    import java.util.ArrayList
    import Exceptions.*
    import scalax.collection.Graph
    import scalax.collection.GraphPredef.*
    import scalax.collection.GraphEdge.*
    import scalax.collection.GraphTraversal.Visitor.empty
    import java.nio.file.Path
    import cats.data.EitherT
    import cats.effect.{IO, Resource}
    import java.lang.IllegalStateException
    
    /*
    Definition of a RDF-Datalog-Program:
    */
    
    type Symbol = String
    
    type Program = Vector[Statement]
    
    implicit val programShow: Show[Program] =
        Show.show(program => (for (s <- program) yield s.show).mkString("\n"))
    
    enum Statement:
        case Fact(head: Literal)
        case Rule(head: Literal, body: Vector[Literal])
    import Statement.*
    
    implicit val statementShow: Show[Statement] =
        Show.show({
            case s: Fact => s.head.show + "."
            case s: Rule => s.head.show + " :- " + s.body.map(_.show).mkString(", ") + "."
        })
    
    type Literal = Tuple2[Symbol, Vector[Term]]
    
    implicit val literalShow: Show[Literal] =
        Show.show(l => {
            val predicateString = l._1 match {
                case "Q" => "Q"
                case ps => "<" + ps.show + ">"
            }
            predicateString + "(" + l._2.map(_.show).mkString(", ") + ")"
        })
    
    enum Term:
        case Constant(n: Symbol)
        case Variable(n: Symbol)
    import Term.*
    
    implicit val termShow: Show[Term] =
        Show.show({
        case t: Constant => t.n match {
            case "Q" => "Q"
            case s => "<" + s + ">"
        }
        case t: Variable => "?" + t.n
    })

    enum Class:
        case LDP
        case LADP(ordered: Program)
        case RDP(cycle: String)

    /*
    Helpers:
    */
    
    /** Returns the literal respresenting a query (by convention thats the rule with the “Q” predicate in the head) or an exception (if there is more or less than one). */
    private[Datalog] def getQuery(p: Program): Either[IllegalStateException, Literal] =
        val candidates = p.filter(statement => statement match {
            case Rule(("Q",_), _) => true
            case _ => false
        })
        candidates match {
            case Vector() => Left(new IllegalStateException("No \"Q\" predicate found in program."))
            case Vector(x) => x.asInstanceOf[Rule].head.asRight[IllegalStateException]
            case _ => Left(new IllegalStateException("More than one \"Q\" predicate found in program."))
        }
        
    /*
    Definition of translation from the rulewerk library:
    */

    /** Translates from the Rulewerk Library to the internal Datalog program.
      * 
      * @param xs A vector of [[org.semanticweb.rulewerk.core.model.api.Statement]].
      * @return Either a [[Exceptions.TranslationException]] describing the error translating or a [[rule_query_translator.Datalog.Program]].
      */
    def fromRulewerk(xs: Vector[RulewerkStatement]): Either[TranslationException, Program] = 
        (for (s <- xs) yield fromRulewerk(s)).sequence
    
    private[Datalog] def fromRulewerk(x: RulewerkStatement): Either[TranslationException, Statement] = x match {
        case x: RulewerkFact =>
            for (l <- fromRulewerk(x.asInstanceOf[RulewerkLiteral]))
                yield (new Fact(l)).asInstanceOf[Statement]
        case x: RulewerkRule =>
            for (head <- {
                val h = x.getHead.getLiterals.asScala
                if h.size != 1
                    then Left(TranslationException("Head with more than one Literal", "x.toString", "Rulewerk", "DatalogProgram"))
                    else fromRulewerk(h(0))};
                body <- x.getBody.getLiterals.asScala.toVector.map(fromRulewerk(_)).sequence)
                yield (new Rule(head, body))
        case x => Left(TranslationException(f"Unknown argument class ${x.getClass}}", "x.toString", "Rulewerk", "DatalogProgram"))
    }
    
    private[Datalog] def fromRulewerk(x: Predicate): Either[TranslationException, Symbol] = x.getName.asRight[TranslationException]
    
    private[Datalog] def fromRulewerk(x: Vector[RulewerkTerm])(implicit d: DummyImplicit): Either[TranslationException, Vector[Term]] =
        def getTermOfType(rt: RulewerkTerm): Either[TranslationException, Term] = rt.getType match {
            case TermType.ABSTRACT_CONSTANT => Constant(rt.getName).asRight[TranslationException]
            case TermType.DATATYPE_CONSTANT => Constant(rt.getName).asRight[TranslationException]
            case TermType.LANGSTRING_CONSTANT => Constant(rt.getName).asRight[TranslationException]
            case TermType.EXISTENTIAL_VARIABLE => Variable(rt.getName).asRight[TranslationException]
            case TermType.UNIVERSAL_VARIABLE => Variable(rt.getName).asRight[TranslationException]
            case TermType.NAMED_NULL => Left(TranslationException("Unexpected Named Null Variable", "x.toString", "Rulewerk", "DatalogProgram"))
        }
        x.map(rt => getTermOfType(rt)).sequence
    
    private[Datalog] def fromRulewerk(x: RulewerkLiteral): Either[TranslationException, Literal] =
        if x.isNegated
            then Left(TranslationException("Negated Literal ${x.getPredicate.getName}", "x.toString", "Rulewerk", "DatalogProgram"))
            else {
                for (p <- fromRulewerk(x.getPredicate);
                    a <- fromRulewerk(x.getArguments.asScala.toVector))
                    yield (new Literal(p,a))
            }
    
    /*
    Definition of translation to the rulewerk library:
    */
    
    /** Translates from the interal Datalog program to the Rulewerk Library.
      * 
      * @param xs A [[rule_query_translator.Datalog.Program]]
      * @return Either a [[Exceptions.TranslationException]] describing the error translating or a vector of [[org.semanticweb.rulewerk.core.model.api.Statement]].
      */
    def toRulewerk(x: Program): Either[TranslationException, Vector[RulewerkStatement]] =
        x.map(toRulewerk(_)).sequence
    
    private[Datalog] def toRulewerk(x: Statement): Either[TranslationException, RulewerkStatement] = x match {
        case x: Fact =>
            for (terms <- toRulewerk(x.head._2);
                predicate <- toRulewerk(x.head._1, terms.size))
                yield FactImpl(predicate, terms.asJava)
        case x: Rule =>
            for (head <- toRulewerk(x.head);
                body <- x.body.map(toRulewerk).sequence)
                yield RuleImpl(ConjunctionImpl(List(head).asJava), ConjunctionImpl(body.toList.asJava))
    }
    
    private[Datalog] def toRulewerk(x: Symbol, arity: Int): Either[TranslationException, Predicate] = PredicateImpl(x, arity).asRight[TranslationException]
    
    private[Datalog] def toRulewerk(x: Vector[Term])(implicit d: DummyImplicit): Either[TranslationException, List[RulewerkTerm]] =
        x.map(toRulewerk).toList.sequence
    
    private[Datalog] def toRulewerk(x: Term): Either[TranslationException, RulewerkTerm] = x match {
        case x: Constant => AbstractConstantImpl(x.n).asRight[TranslationException]
        case x: Variable => UniversalVariableImpl(x.n).asRight[TranslationException]
    }
    
    private[Datalog] def toRulewerk(x: Literal): Either[TranslationException, PositiveLiteral] =
        for (terms <- toRulewerk(x._2);
            predicate <- toRulewerk(x._1, terms.size))
            yield PositiveLiteralImpl(predicate, terms.asJava)
    
    /*
    Parse from String:
    */
    
    /** Parses a Program from a given String.
      * 
      * @param s The String to parse from, needs to contain a valid listing of Datalog rules and facts.
      * @return Either a [[Exceptions.ParsingException]] describing the error parsing or a datalog program.
      */
    def parseProgram(s: String): Either[ParsingException, Program] =
        for
            knowledgeBase <- Try(RuleParser.parse(s)).fold(
                e => Left(ParsingException("Parsing the Datalog program with the RDF4J parser was unsuccessful.", s, "DatalogProgram", Some(e))),
                v => Right(v)
            )
            rules = knowledgeBase.getRules().asScala.toVector.map(_.asInstanceOf[RulewerkStatement])
            facts = knowledgeBase.getFacts().asScala.toVector.map(_.asInstanceOf[RulewerkStatement])
            statements <- fromRulewerk(facts ++ rules).fold(
                l => ParsingException("Translating parsed statements was unsuccessfull.", s, "DatalogProgram", Some(l)).asLeft[Program],
                r => r.asRight[ParsingException]
            )
        yield
            statements
    
    /** Parses a Fact from a given String.
      * 
      * @param s The String to parse from, needs to contain a valid Datalog fact.
      * @return Either a [[Exceptions.ParsingException]] describing the error parsing or a datalog fact.
      */
    def parseFact(s: String): Either[ParsingException, Statement] =
        fromRulewerk(RuleParser.parseFact(s).asInstanceOf[RulewerkStatement]).fold(
            l => ParsingException("Translating parsed fact was unsuccessfull.", s, "DatalogProgram", Some(l)).asLeft[Statement],
            r => r.asRight[ParsingException]
        )
    
    /** Parses a Rule from a given String.
      * 
      * @param s The String to parse from, needs to contain a valid Datalog rule.
      * @return Either a [[Exceptions.ParsingException]] describing the error parsing or a datalog rule.
      */
    def parseRule(s: String): Either[ParsingException, Statement] =
        fromRulewerk(RuleParser.parseRule(s).asInstanceOf[RulewerkStatement]).fold(
            l => ParsingException("Translating parsed rule was unsuccessfull.", s, "DatalogProgram", Some(l)).asLeft[Statement],
            r => r.asRight[ParsingException]
        )

    /*
    Run a Datalog Program.
    */

    /** Evaluates the given query on the given data. (No OWL or RDFS inference.)
      * 
      * As the evaluation uses the Rulewerk Library, the data and query must be given in the Classes of this library.
      * 
      * @param data The data (in Form of Rules and Statements) that holds.
      * @param query The query to evaluate, a positive literal with one or more variables.
      * @return A Vector of solutions for the variables in the query literal (or the occured exception).
      */
    def evaluate(data: Vector[RulewerkStatement], query: PositiveLiteral): Try[Vector[Vector[String]]] =
        val kb = new KnowledgeBase()
        kb.addStatements(data.asJava)
        for
            reasoner <- Try {
                val reasoner = new VLogReasoner(kb)
                reasoner.reason()
                reasoner
            }
            answers <- Try {
                val answers = reasoner.answerQuery(query, true)
                val list = new ArrayList[QueryResult]()
                answers.forEachRemaining(list.add)
                list.asScala.toVector.map(_.getTerms.asScala.toVector.map(_.getName))
            }
        yield
            answers

    /** Evaluates the given program.
      * 
      * For the program the literal with the predicate “Q” is extracted and used as the target query.
      * 
      * @param path Path to an RDF file in TURTLE syntax with additional data (optional).
      * @param program The program to evaluate, must contain the convention predicate “Q”.
      * @return Either a [[Exceptions.EvaluationException]] describing the error that occured, or a Vector of solutions for the variables in the “Q” literal.
      */
    def evaluateQuery(program: Program, path: Option[Path]): IO[Either[EvaluationException, Vector[Vector[String]]]] =
        EitherT.right(path.map(RDF.toDatalog(_)).sequence).flatMap((data: Option[Vector[RulewerkStatement]]) => EitherT.fromEither(evaluateQuery(program, data))).value

    /** Evaluates the given program on the given data. (No OWL or RDFS inference.)
      * 
      * As the evaluation uses the Rulewerk Library, the data must be given in the classes of this library.
      * For the program the literal with the predicate “Q” is extracted and used as the target query.
      * 
      * @param data The data (in Form of Rules and Statements) that holds in addition to the program (optional).
      * @param program The program to evaluate, must contain the convention predicate “Q”.
      * @return Either a [[Exceptions.EvaluationException]] describing the error that occured, or a Vector of solutions for the variables in the “Q” literal.
      */
    private[Datalog] def evaluateQuery(program: Program, data: Option[Vector[RulewerkStatement]]): Either[EvaluationException, Vector[Vector[String]]] =
        for 
            query <- getQuery(program).fold(
                l => Left(EvaluationException(l.getMessage(), program.show, "DatalogProgram", Some(l))),
                r => r.asRight[EvaluationException]
            )
            queryLiteral <- toRulewerk(query).fold(
                l => Left(EvaluationException("Translating query to Rulewerk was unsuccessfull.", program.show, "DatalogProgram", Some(l))),
                r => r.asRight[EvaluationException]
            )
            statements <- toRulewerk(program).fold(
                l => Left(EvaluationException("Translating program to Rulewerk was unsuccessfull.", program.show, "DatalogProgram", Some(l))),
                r => r.asRight[EvaluationException]
            )
            wholeData = data.getOrElse(Vector()) ++ statements
            answer <- evaluate(wholeData, queryLiteral).transform(
                        s => Success(Right(s)),
                        f => Success(Left(EvaluationException("Evaluation of program by Rulewerk Library was unsuccessfull.", program.show, "DatalogProgram", Some(f))))
                      ).get
        yield
            answer
    
    /*
    Program Classes:
    */
    
    private[Datalog] type DependecyGraph = Graph[Symbol, DiEdge]

    private[Datalog] def getDependecyGraph(program: Program): DependecyGraph =
        val nodes = program.map(_ match {
            case Rule(head, _) => head._1
            case Fact(head) => head._1
        }).filter(_ != "Q")
        val edges: Vector[DiEdge[String]] =
            program
                .filter(_.isInstanceOf[Rule])
                .filter(_.asInstanceOf[Rule].head._1 != "Q")
                .map(_ match {
                    case Rule(head, body) => body.map(
                        x => DiEdge(head._1, x._1)
                    )
                }).flatten
        Graph.from(nodes, edges)

    /** Determines the (complexity) class of the given program.
      * 
      * @param program The program to analyse.
      * @return Either a [[Exceptions.EvaluationException]] describing the error that occured, or the [[Class]] of the program, possibly containing further information (the ordered program for LADP or a cycle description for RDP).
      */
    def getProgramClass(p: Program): Either[AnalysingException, Class] =
        def getCycle(graph: Graph[Symbol, DiEdge]) = 
            graph
                .findCycle(empty)
                .map(_.edges.map(e => "<" + e._1.toString + "> depends on <" + e._2.toString + ">").mkString(" and "))
                .map(Class.RDP.apply)
                .map(_.asRight[AnalysingException])
                .getOrElse(
                    AnalysingException("Could not find expected cycle in dependency graph", p.show, "DatalogProgram").asLeft[Class])
        
        val graph = getDependecyGraph(p)
        if graph.isEmpty then
            Class.LDP.asRight[AnalysingException]
        else
            val components = graph.componentTraverser()
            val maxLength = components.maxBy(_.nodes.size).nodes.size
            maxLength match {
                case 2 => if graph.isCyclic then getCycle(graph) else Class.LDP.asRight[AnalysingException]
                case x if x < 2 => Class.LDP.asRight[AnalysingException] // No component with two or more nodes
                case x if x > 2 => 
                    graph.topologicalSort(empty).fold(
                        cycleNode => getCycle(graph),
                        order => {
                            val layers = order.toLayered
                            val highestLayer = layers.maxBy(_._1)._1
                            Try {
                                Class.LADP(p.sortBy((s: Statement) => {
                                    val predicate = s match {
                                        case f: Fact => "fact"
                                        case r: Rule => r.head._1
                                    }
                                    if predicate == "Q" then
                                        0
                                    else if predicate == "fact" then
                                        highestLayer + 1
                                    else
                                        layers.find(_._2.exists(_.value == predicate)).get._1
                                })(Ordering[Int].reverse))
                            }.fold(
                                (t: Throwable) => Left(AnalysingException("Error sorting program topologically", p.show, "DatalogProgram", Option(t))),
                                _.asRight[AnalysingException]
                            )
                        }
                    )
            }
}