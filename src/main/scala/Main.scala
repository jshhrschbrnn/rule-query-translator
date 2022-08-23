package rule_query_translator

import com.monovore.decline.*
import com.monovore.decline.effect.*
import java.nio.file.Path
import java.io.*
import cats.*
import cats.syntax.show.*
import cats.effect.*
import cats.implicits.*
import cats.effect.kernel.Resource
import java.io.FileInputStream
import java.nio.file.Files
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.stream.Collectors
import Translation.*
import java.io.PrintWriter
import SPARQL.*
import Datalog.*
import org.eclipse.rdf4j.rio.RDFFormat
import Exceptions.*

/** The main object of the software.
  * 
  * It provides a command line interface for translating between SPARQL and Datalog files.
  * For usage information run {@code program --help}.
  */
object TranslatorApp extends CommandIOApp(
    name = "rqt",
    header = "Translate between Datalog Rules and SPARQL Queries.",
    version = "0.0.1"
) {
    private[TranslatorApp] val inputFile = Opts.option[Path]("input", short = "i", metavar = "inputFile", help = "The file to translate from.")
    private[TranslatorApp] val outputFile = Opts.option[Path]("output", short = "o", metavar = "outputFile", help = "The file to translate to.")
    private[TranslatorApp] val databaseFile = Opts.option[Path]("database", short = "d", metavar = "databaseFile", help = "The RDF Database to use when evaluating queries or rules (Turtle Format) (implies evaluate).")
    private[TranslatorApp] val language = Opts.option[String]("type", short = "t", metavar = "sourceType", help = "The type (“S” (SPARQL) or “D” (Datalog)) of the source file (the target will be the other one).").validate("Type must be “S” or “D”") {s => s == "S" || s == "D"}
    private[TranslatorApp] val verbose = Opts.flag("verbose", short = "v", help = "Print more information.")
    private[TranslatorApp] val evaluate = Opts.flag("evaluate", short = "e", help = "Evaluate the Script and Program (using the Database file, if given.)")
    private[TranslatorApp] val force = Opts.flag("force", short = "f", help = "Force the translation, even if a correct output can not be guranteed.")
    
    private[TranslatorApp] case class Config(
        input: Path,
        output: Path,
        database: Option[Path],
        language: String,
        verbose: Boolean,
        evaluate: Boolean,
        force: Boolean
    )

    /** Combines CLI Arguments to [[Config]]. */
    private[TranslatorApp] val configOpts: Opts[Config] =
        (inputFile, outputFile, databaseFile.orNone, language, verbose.orFalse, evaluate.orFalse, force.orFalse).mapN(Config.apply)

    override def main: Opts[IO[ExitCode]] =
        configOpts.map(config => {
            for
                input <- fileInputStream(config.input).use { case in => readFromStream(in) }
                parsed <- if config.language == "S" then eitherToIo(SPARQL.parseString(input))
                                                    else eitherToIo(Datalog.parseProgram(input))
                _ <- if config.verbose then IO.println("Parsed Input to:") >> IO.println((parsed: Program | Script).show) else IO.unit
                translation <- eitherToIo(translate(parsed, config.force))
                _ <- if config.verbose then IO.println("Translated Input to:") >> IO.println(translation.show) else IO.unit
                _ <- fileOutputStream(config.output).use { case out => writeToStream(translation.show, out) }
                _ <- (config.database, config.evaluate) match {
                    case (None, false) => IO.unit
                    case (p, e) if (p.isDefined || e) => {
                        for
                            answerDatalog <- Datalog.evaluateQuery(
                                if config.language == "S" then translation.asInstanceOf[Datalog.Program] else parsed.asInstanceOf[Datalog.Program],
                                p
                            ).flatMap(eitherToIo)
                            answerSPARQL <- SPARQL.evaluateQuery(
                                if config.language == "S" then parsed.asInstanceOf[SPARQL.Script] else translation.asInstanceOf[SPARQL.Script],
                                p
                            ).flatMap(eitherToIo)
                            _ <- IO.println("Answers to Datalog evaluation:") >> IO.println(answerDatalog)
                            _ <- IO.println("Answers to SPARQL evaluation:") >> IO.println(answerSPARQL)
                        yield
                            IO.unit
                    }
                }
            yield
                ExitCode.Success
        }.handleErrorWith(f => {
            if config.verbose then
                IO(f.printStackTrace()).as(ExitCode.Error) else {
                    f match {
                        case e: RQTException => IO.println(f.getMessage())
                        case e => IO.println("Error: " + e.toString())
                    }
                }.as(ExitCode.Error)
        }))
    
    /** Writes a String to a given [[OutputStream]]. */
    private[TranslatorApp] def writeToStream(s: String, out: OutputStream): IO[Unit] =
        val writer = Resource.make {
            IO.blocking(new PrintWriter(out))
        } { writer =>
            IO.blocking(writer.close()).handleErrorWith(_ => IO.println("Error closing PrintWriter"))
        }
        writer.use {case w => IO.blocking(w.println(s))}
    
    /** Reads a String from the given [[InputStream]]. */
    private[TranslatorApp] def readFromStream(in: InputStream): IO[String] =
        val reader = Resource.make {
            IO.blocking(new BufferedReader(new InputStreamReader(in)))
        } { reader => 
            IO.blocking(reader.close()).handleErrorWith(_ => IO.println("Error closing BufferedReader"))
        }
        reader.use {case r => IO.blocking(r.lines().parallel().collect(Collectors.joining("\n")))}
    
    /** Opens a new managed ([[Resource]]) [[FileInputStream]]. */
    private[TranslatorApp] def fileInputStream(f: Path): Resource[IO, InputStream] =
        Resource.make {
            IO.blocking(Files.newInputStream(f))
        } { inStream =>
            IO.blocking(inStream.close()).handleErrorWith(_ => IO.println("Error closing FileInputStream"))
        }
    
    /** Opens a new managed ([[Resource]]) [[FileOutputStream]]. */
    private[TranslatorApp] def fileOutputStream(f: Path): Resource[IO, OutputStream] =
        Resource.make {
            IO.blocking(Files.newOutputStream(f))
        } { outStream =>
            IO.blocking(outStream.close()).handleErrorWith(_ => IO.println("Error closing FileOutputStream"))
        }
}