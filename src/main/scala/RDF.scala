package rule_query_translator

import org.eclipse.rdf4j.sparqlbuilder.core.QueryElement

package object RDF {

    import java.nio.file.Path
    import org.semanticweb.owlapi.apibinding.OWLManager;
    import org.semanticweb.owlapi.model.OWLOntology;
    import org.semanticweb.owlapi.model.OWLOntologyCreationException;
    import org.semanticweb.owlapi.model.OWLOntologyManager;
    import org.semanticweb.rulewerk.owlapi.OwlToRulesConverter;
    import org.semanticweb.rulewerk.core.model.api.{Statement => RulewerkStatement}
    import scala.jdk.CollectionConverters.*
    import cats.effect.{IO, Resource}
    import cats.syntax.all.*
    import org.eclipse.rdf4j.repository.RepositoryConnection
    import org.eclipse.rdf4j.repository.Repository
    import org.eclipse.rdf4j.repository.sail.SailRepository
    import org.eclipse.rdf4j.sail.memory.MemoryStore
    import org.eclipse.rdf4j.rio.RDFFormat
    import java.io.InputStream
    import java.nio.file.Files
    import org.eclipse.rdf4j.model.{Statement => Rdf4jStatement}

    /** Read a RDF File to a collection of [[org.semanticweb.rulewerk.core.model.api.Statement]]. (No OWL or RDFS inference.)*/
    def toDatalog(p: Path): IO[Vector[RulewerkStatement]] =
        val ontologyManager: OWLOntologyManager = OWLManager.createOWLOntologyManager()
        val owlToRulesConverter = new OwlToRulesConverter()
        for
            ontology <- IO.blocking(ontologyManager.loadOntologyFromOntologyDocument(p.toFile))
            _ <- IO(owlToRulesConverter.addOntology(ontology))
            facts = owlToRulesConverter.getFacts.asScala.toVector
        yield
            facts

    /** Returns a new [[org.eclipse.rdf4j.repository.Repository]], managed as a [[cats.effect.Resource]]. */
    def newRepository(): Resource[IO, Repository] =
        Resource.make {
            IO.blocking(new SailRepository(new MemoryStore()))
        } {r => 
            IO.blocking(r.shutDown()).handleErrorWith(_ => IO.println("Error closing Repository"))
        }
    
    /** Opens a new managed ([[Resource]]) [[FileInputStream]]. */
    private[RDF] def fileInputStream(f: Path): Resource[IO, InputStream] =
        Resource.make {
            IO.blocking(Files.newInputStream(f))
        } { inStream =>
            IO.blocking(inStream.close()).handleErrorWith(_ => IO.println("Error closing FileInputStream"))
        }

    /** Adds the given RDF File as data to the given [[org.eclipse.rdf4j.repository.Repository]].
      * 
      * @param p The path of the RDF File to load.
      * @param repo The Repository to use.
      * @param format The [[org.eclipse.rdf4j.rio.RDFFormat]] of the RDF File.
      */
    def addRDFToRepository(p: Path, repo: Repository, format: RDFFormat): IO[Unit] =
        fileInputStream(p).use {
            case input => {
                val connection = Resource.fromAutoCloseable(IO(repo.getConnection()))
                connection.use {
                    case conn => IO.blocking(conn.add(input, "", format))
                }
            }
        }
    
    /** Adds the given insert queries of type [[org.eclipse.rdf4j.sparqlbuilder.core.QueryElement]] as data to the given [[org.eclipse.rdf4j.repository.Repository]].
      * 
      * @param inserts A vector of QueryElements.
      * @param repo The Repository to use.
      */
    def addDataToRepository(inserts: Vector[QueryElement], repo: Repository): IO[Unit] =
        val connection = Resource.fromAutoCloseable(IO(repo.getConnection()))
        connection.use {
            case conn => {
                IO.blocking {
                    val updates = {
                        for
                            insertQuery <- inserts
                        yield
                            conn.prepareUpdate(insertQuery.getQueryString)
                    }
                    updates.map(_.execute)
                }
            }
        }
}