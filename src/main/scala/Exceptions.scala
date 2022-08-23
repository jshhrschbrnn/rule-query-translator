package rule_query_translator
import cats.effect.*

package object Exceptions {

    private[Exceptions] def optionToNull[A >: Null](x: Option[A]): A = x match {
        case Some(y) => y
        case None => null
    }

    def eitherToIo[A](x: Either[Throwable, A]): IO[A] = x match {
        case Left(e) => IO.raiseError(e)
        case Right(a) => IO.pure(a)
    }

    class RQTException(message: String, causing: Throwable) extends Exception(message, causing)
    
    case class ParsingException(description: String, input: String, target: String, causing: Option[Throwable] = None) extends RQTException(f"Error parsing to $target – $description – in ${'"'}$input${'"'}.", optionToNull(causing))
    case class TranslationException(description: String, input: String, source: String, target: String, causing: Option[Throwable] = None) extends RQTException(f"Error translating from $source to $target – $description – in ${'"'}$input${'"'}.", optionToNull(causing))
    case class EvaluationException(description: String, input: String, language: String, causing: Option[Throwable] = None) extends RQTException(f"Error evaluating $language – $description – in ${'"'}$input${'"'}.", optionToNull(causing))
    case class AnalysingException(description: String, input: String, language: String, causing: Option[Throwable] = None) extends RQTException(f"Error analysing $language – $description – in ${'"'}$input${'"'}.", optionToNull(causing))

}