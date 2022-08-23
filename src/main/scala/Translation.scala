package rule_query_translator

/** Provides methods to translate between a Datalog Program and a SPARQL Script.
  * 
  * ## From Datalog to SPARQL
  * To translate from a Datalog Program to a SPARQL Script, the method [[translateToSparql(Program)]] can be used.
  * 
  * ## From SPARQL to Datalog
  * To translate from a SPARQL Script to a Datalog Program, the method [[translateToDatalog(Script)]] can be used.
  * 
  * ## Universal
  * To automatically detect the target language, use [[translate(input)]].
  */ 
package object Translation {
    
    import rule_query_translator.SPARQL.{Term => STerm, *}
    import rule_query_translator.Datalog.{Term => DTerm, *}
    import rule_query_translator.Datalog.Statement.*
    import cats.*, cats.data.*, cats.implicits.*
    import Exceptions.*

    /* Rdf iri for beeing of a type. */
    val TypeTerm = STerm.Constant("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

    /** Translates from a given language (SPARQL or Datalog) to the other one.
      * 
      * @param input Either a [[rule_query_translator.Datalog.Program]] or a [[rule_query_translator.SPARQL.Script]].
      * @param force Whether to force the translation if it is not possible to translate in a faithfull way.
      * @return Either a [[Exceptions.TranslationException]] describing the error translating or the corresponding translation.
      */
    def translate(input: Program | Script, force: Boolean): Either[TranslationException, Program | Script] =
        for
            firstElement <- if input.isEmpty then Left(TranslationException("The input to translate was an empty Vector.", input.show, "{unknown}", "{unkown}")) else Right(input(0))
            translation <- firstElement match {
                    case fe: Statement => translateToSparql(input.asInstanceOf[Program], force)
                    case fe: Query  => translateToDatalog(input.asInstanceOf[Script], force)
            }
        yield
            translation

    implicit val translationShow: Show[Program | Script] =
        Show.show(input => {
                if input.isEmpty then ""
                                 else {
                    input(0) match {
                            case fe: Statement => input.asInstanceOf[Program].show
                            case fe: Query  => input.asInstanceOf[Script].show
                    }
            }
        })

    /** Translates from a Datalog Program to a SPARQL Script.
      * 
      * @param p A [[rule_query_translator.Datalog.Program]].
      * @param force Whether to force the translation if it is not possible to translate in a faithfull way.
      * @return Either a [[Exceptions.TranslationException]] describing the error translating or a [[rule_query_translator.SPARQL.Script]].
      */
    def translateToSparql(p: Program, force: Boolean): Either[TranslationException, Script] =
        def translate(p: Program) = (for (s <- p) yield translateToSparql(s)).sequence.map(SPARQL.combineInsertDatas)
        Datalog.getProgramClass(p).fold(
            l => TranslationException("Error determining program class", p.show, "DatalogProgram", "SPARQLScript", Some(l)).asLeft[Script],
            r => r match {
                case Datalog.Class.LDP => translate(p)
                case Datalog.Class.LADP(op) => translate(op)
                case Datalog.Class.RDP(cycle) => if force then
                    translate(p) else
                    Left(TranslationException(f"Can not translate recursive program (RDP): $cycle", p.show, "DatalogProgram", "SPARQLScript"))
            }
        )
    
    private[Translation] def translateToSparql(s: Statement): Either[TranslationException, Query] = s match {
        case Rule((symbol, terms), body) if symbol == "Q" =>
            for (vs <- translateToSparql(terms);
                tb <- translateToSparql(body);
                result <-
                    if containsConstant(vs) then Left(TranslationException("Unexpected Constant in Final Query Statement", s.show, "DatalogProgram", "SPARQLScript")) else Query.Select(vs, tb).asRight[TranslationException]
            )
            yield result
        case Fact(head) =>
            for (ts <- translateToSparql(head))
            yield Query.InsertData(Vector(ts))
        case Rule(head, body) =>
            for (ts <- translateToSparql(head); tb <- translateToSparql(body))
            yield Query.Insert(Vector(ts), tb)
    }
    
    private[Translation] def translateToSparql(ls: Vector[Literal])(implicit d: DummyImplicit): Either[TranslationException, Vector[Triple]] =
        (for (l <- ls) yield translateToSparql(l)).sequence
    
    private[Translation] def translateToSparql(l: Literal): Either[TranslationException, Triple] = l match {
        case (s, Vector(t1)) =>
            for (nt1 <- translateToSparql(t1))
            yield (nt1, TypeTerm, STerm.Constant(s))
        case (s, Vector(t1, t2)) =>
            for (nt1 <- translateToSparql(t1); nt2 <- translateToSparql(t2))
            yield (nt1, STerm.Constant(s), nt2)
        case (s, _) => Left(TranslationException("Too many Terms (>2) in Literal", l.show, "DatalogProgram", "SPARQLScript"))
    }
    
    private[Translation] def translateToSparql(ts: Vector[DTerm])(implicit d: DummyImplicit, d2: DummyImplicit): Either[TranslationException, Vector[STerm]] =
        (for (t <- ts) yield translateToSparql(t)).sequence
    
    private[Translation] def translateToSparql(t: DTerm): Either[TranslationException, STerm] = t match {
        case DTerm.Constant(c) => STerm.Constant(c).asInstanceOf[STerm].asRight[TranslationException]
        case DTerm.Variable(v) => STerm.Variable(v).asInstanceOf[STerm].asRight[TranslationException]
    }

    /** Returs whether the given vector of terms contains a constant. */
    private[Translation] def containsConstant(ts: Vector[STerm]): Boolean = ts.exists({
        case STerm.Constant(_) => true
        case STerm.Variable(_) => false
    })
    
    /** Translates from a SPARQL Script to a Datalog Program.
      * 
      * @param p A [[rule_query_translator.SPARQL.Script]].
      * @param force Whether to force the translation if it is not possible to translate in a faithfull way.
      * @return Either a [[Exceptions.TranslationException]] describing the error translating or a [[rule_query_translator.Datalog.Program]].
      */
    def translateToDatalog(s: Script, force: Boolean): Either[TranslationException, Program] =
        def translate(s: Script) = s.map(translateToDatalog).sequence.map(_.flatten)
        SPARQL.getScriptClass(s) match {
            case SPARQL.Class.DSS => translate(s)
            case SPARQL.Class.ISS(predicates) =>  if force then
                    translate(s) else
                    Left(TranslationException(f"Can not translate imperative script (ISS), problematic predicates are: ${predicates.map(_.show).mkString(", ")}", s.show, "SPARQLScript", "DatalogProgram"))
        }
    
    private[Translation] def translateToDatalog(q: Query): Either[TranslationException, Vector[Statement]] = q match {
        case Query.Select(variables, triplesBlock) =>
            for
                terms <- variables.map(translateToDatalog).sequence
                body <- triplesBlock.map(translateToDatalog).sequence.filterOrElse(_.nonEmpty, TranslationException("Select with empty body.", q.show, "SPARQLScript", "DatalogProgram"))
            yield
                Vector(Statement.Rule(("Q", terms), body))
        case Query.Insert(triples, triplesBlock) =>
            {
                for
                    triple <- triples
                yield
                    triplesBlock match {
                        case Vector() =>
                            Left(TranslationException("Recieved INSERT query with empty WHERE.", q.show, "SPARQLScript", "DatalogProgram"))
                        case tb =>
                            for
                                head <- translateToDatalog(triple)
                                body <- tb.map(translateToDatalog).sequence
                            yield
                                Statement.Rule(head, body)
                    }
            }.sequence
        case Query.InsertData(triples) =>
            {
                for
                    triple <- triples
                yield
                    for (head <- translateToDatalog(triple))
                    yield Statement.Fact(head)
            }.sequence
    }
    
    private[Translation] def translateToDatalog(t: Triple): Either[TranslationException, Literal] =
        t match {
            case (STerm.Constant(c1), TypeTerm, STerm.Constant(c3)) =>
                (c3, Vector(DTerm.Constant(c1))).asRight[TranslationException]
            case (STerm.Constant(c1), STerm.Constant(c2), STerm.Constant(c3)) =>
                (c2, Vector(DTerm.Constant(c1), DTerm.Constant(c3))).asRight[TranslationException]
            case (STerm.Constant(c1), TypeTerm, STerm.Variable(v3)) =>
                // ("Triple", Vector(DTerm.Constant(c1), DTerm.Constant("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"), DTerm.Variable(v3))).asRight[String]
                Left(TranslationException(f"Can not translate a type assignment with a variable type.", t.show, "SPARQLScript", "DatalogProgram"))
            case (STerm.Constant(c1), STerm.Constant(c2), STerm.Variable(v3)) =>
                (c2, Vector(DTerm.Constant(c1), DTerm.Variable(v3))).asRight[TranslationException]
            case (STerm.Constant(c1), STerm.Variable(v2), STerm.Constant(c3)) =>
                // ("Triple", Vector(DTerm.Constant(c1), DTerm.Variable(v2), DTerm.Constant(c3))).asRight[String]
                Left(TranslationException(f"Can not translate a type assignment with a variable predicate.", t.show, "SPARQLScript", "DatalogProgram"))
            case (STerm.Constant(c1), STerm.Variable(v2), STerm.Variable(v3)) =>
                // ("Triple", Vector(DTerm.Constant(c1), DTerm.Variable(v2), DTerm.Variable(v3))).asRight[String]
                Left(TranslationException(f"Can not translate a type assignment with a variable predicate.", t.show, "SPARQLScript", "DatalogProgram"))
            case (STerm.Variable(v1), TypeTerm, STerm.Constant(c3)) =>
                (c3, Vector(DTerm.Variable(v1))).asRight[TranslationException]
            case (STerm.Variable(v1), STerm.Constant(c2), STerm.Constant(c3)) =>
                (c2, Vector(DTerm.Variable(v1), DTerm.Constant(c3))).asRight[TranslationException]
            case (STerm.Variable(v1), TypeTerm, STerm.Variable(v3)) =>
                // ("Triple", Vector(DTerm.Variable(v1), DTerm.Constant("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"), DTerm.Variable(v3))).asRight[String]
                Left(TranslationException(f"Can not translate a type assignment with a variable type.", t.show, "SPARQLScript", "DatalogProgram"))
            case (STerm.Variable(v1), STerm.Constant(c2), STerm.Variable(v3)) =>
                (c2, Vector(DTerm.Variable(v1), DTerm.Variable(v3))).asRight[TranslationException]
            case (STerm.Variable(v1), STerm.Variable(v2), STerm.Constant(c3)) =>
                // ("Triple", Vector(DTerm.Variable(v1), DTerm.Variable(v2), DTerm.Constant(c3))).asRight[String]
                Left(TranslationException(f"Can not translate a type assignment with a variable predicate.", t.show, "SPARQLScript", "DatalogProgram"))
            case (STerm.Variable(v1), STerm.Variable(v2), STerm.Variable(v3)) =>
                // ("Triple", Vector(DTerm.Variable(v1), DTerm.Variable(v2), DTerm.Variable(v3))).asRight[String]
                Left(TranslationException(f"Can not translate a type assignment with a variable predicate.", t.show, "SPARQLScript", "DatalogProgram"))
        }
    
    private[Translation] def translateToDatalog(t: STerm): Either[TranslationException, DTerm] = t match {
        case STerm.Constant(c) => DTerm.Constant(c).asInstanceOf[DTerm].asRight[TranslationException]
        case STerm.Variable(v) => DTerm.Variable(v).asInstanceOf[DTerm].asRight[TranslationException]
    }
}