# rule-query-translator

Translate between a collection of SPARQL queries and a Datalog program.

## Description & Motivation

A number of languages for querying and manipulating data sets exist. In the
realm of Semantic Web, two such languages are frequently used. SPARQL queries
allow retrieving and also adding facts to data sets, while Datalog rule engines can
derive logical consequences. Often a combination of SPARQL INSERT queries can achieve
the same result as a collection of Datalog rules and vice versa. At the same
time, the performance between the two can vary. Therefore, users often manually
translate between the two languages, losing valuable time. In this project, I
implemented an automatic translation between a
collection of SPARQL INSERT queries with a final SELECT query (a SPARQL Script) and a collection of Datalog
facts and rules (a Datalog Program).

The translation is
implemented in a command line application, allowing to automate the translation
process between SPARQL and Datalog.
Given an input file containing either a Datalog Program or a SPARQL Script, the application writes a
translation to a given output file.
Additionally, the application is able to evaluate the Program/Script on the data
of a given RDF file, before and after translation.

### Example

For example, the following SPARQL Script would be translated to the following Datalog Program (without the application of the prefix):

```sparql
PREFIX ex: <http://example.org#>.

INSERT DATA
{
  ex:ethel ex:parent ex:alan.
  ex:ethel ex:parent ex:john.
}

INSERT
{
  ?x ex:sibling ?y.
}
WHERE
{
  ?z ex:parent ?x.
  ?z ex:parent ?y.
}

SELECT ?x
{
  ?x ex:sibling ex:alan.
}
```

```prolog
@prefix ex: <http://example.org#>.

ex:parent(ex:ethel, ex:alan).
ex:parent(ex:ethel, ex:john).

ex:sibling(?x,?y) :- ex:parent(?z,?x), ex:parent(?z,?y).

Q(?x) :- ex:sibling(?x, ex:alan).
```

To do so the tool would be used in the following way:

```bash
rqt -i ./siblings.dl -o ./siblings.rq -t S
```

## Building & Running

The rule-query-translator is build using [sbt](https://www.scala-sbt.org/).

Use `sbt assembly` to build an executable jar in `target/scala-3.1.2` or `sbt run` to run the tool directly.

Tests can be run using `sbt test`.

## Usage

```
Usage: rqt --input <inputFile> --output <outputFile> [--database <databaseFile>] --type <sourceType> [--verbose] [--evaluate] [--force]

Translate between Datalog Rules and SPARQL Queries.

Options and flags:
    --help
        Display this help text.
    --version, -v
        Print the version number and exit.
    --input <inputFile>, -i <inputFile>
        The file to translate from.
    --output <outputFile>, -o <outputFile>
        The file to translate to.
    --database <databaseFile>, -d <databaseFile>
        The RDF Database to use when evaluating queries or rules (Turtle Format) (implies evaluate).
    --type <sourceType>, -t <sourceType>
        The type (“S” (SPARQL) or “D” (Datalog)) of the source file (the target will be the other one).
    --verbose, -v
        Print more information.
    --evaluate, -e
        Evaluate the Script and Program (using the Database file, if given.)
    --force, -f
        Force the translation, even if a correct output can not be guranteed.
```
