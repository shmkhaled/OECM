# OECM
OECM (Ontology Enrichment using Cross-lingual Matching) is used for enriching an ontology, the target ontology T, using another one, the source ontology S, in a different natural language.

All implementations are based on Scala 2.11.11 and Apache Spark 2.3.1. 

How to use
----------
````
git clone https://github.com/shmkhaled/OECM.git
cd OECM

mvn clean package
````

The subsequent steps depend on your IDE. Generally, just import this repository as a Maven project and start using OECM.

Program arguments
----------
arg(0) = source ontology (.nt)<br />
Example: "src/main/resources/EvaluationDataset/German/conference-de-classes_updated.nt"

arg (1) = target ontology (.nt)<br />
Example: arg(1) = "src/main/resources/CaseStudy/SEO_classes.nt"

arg (2) = English translations for source ontology (.csv)<br />
Example: arg(2) = "src/main/resources/EvaluationDataset/Translations/Translations-conference-de.csv"
