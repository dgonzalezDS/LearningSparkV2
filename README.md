# Proyecto: Ejercicios de Learning Spark V2 en Scala
Este proyecto es una implementación práctica de los ejercicios y ejemplos del libro Learning Spark V2, utilizando Scala como lenguaje de programación y Apache Spark como motor de procesamiento distribuido. El proyecto está estructurado en IntelliJ IDEA y utiliza Maven como herramienta de gestión de dependencias.

## Estructura del Proyecto
El proyecto está organizado de la siguiente manera:

Objeto Principal (App): El punto de entrada del proyecto es el objeto App, que se encarga de ejecutar los ejercicios según los parámetros proporcionados (capítulo y número de ejercicio).

Objetos por Capítulo: Cada capítulo del libro tiene su propio objeto (por ejemplo, chapter2, chapter3, etc.). Dentro de cada objeto, se definen métodos que corresponden a los ejercicios del capítulo (por ejemplo, ejercicio1, ejercicio2, etc.).

SparkSession: Se crea una única instancia de SparkSession al inicio del programa, que se utiliza en todos los ejercicios. Esta instancia se cierra automáticamente al finalizar la ejecución.

## Requisitos
Java 8+: Apache Spark requiere Java 8 o superior.

Scala 2.12: El proyecto está configurado para usar Scala 2.12, que es compatible con Spark 3.x.

Apache Spark 3.x: Las dependencias de Spark se gestionan mediante Maven.

IntelliJ IDEA: El proyecto está configurado para ser abierto y ejecutado en IntelliJ IDEA.

## Cómo Ejecutar los Ejercicios
### Clonar el Repositorio:

git clone https://github.com/dgonzalezDS/LearningSparkV2.git

cd /dgonzalezDS/LearningSparkV2 (modificar según la ruta donde hayas clonado el repo)

### Abrir el Proyecto en IntelliJ:

Abre IntelliJ IDEA y selecciona Open.

Navega hasta el directorio del proyecto y selecciona el archivo pom.xml.

### Ejecutar un Ejercicio:

El proyecto se ejecuta desde el objeto App.

Para ejecutar un ejercicio específico, proporciona los argumentos correspondientes al capítulo y al número de ejercicio. Por ejemplo:

mvn exec:java -Dexec.mainClass="App" -Dexec.args="2 1"
Esto ejecutará el ejercicio 1 del capítulo 2.

### Ejecutar desde IntelliJ (recomendado):

Configura una nueva configuración de ejecución en IntelliJ:

Clase principal: App.

Argumentos del programa: <capítulo> <ejercicio> (por ejemplo, 2 1).

## Estructura de Archivos

src/

├── main/

│   └── scala/

│       ├── App.scala                # Objeto principal

│       ├── chapter2/                # Ejercicios del capítulo 2

│       │   └── Chapter2.scala

│       ├── chapter3/                # Ejercicios del capítulo 3

│       │   └── Chapter3.scala

│       └── ...                      # Resto de capítulos

pom.xml                              # Archivo de configuración de Maven

README.md                            # Este archivo


## Dependencias
Las dependencias del proyecto, incluyendo Apache Spark, se gestionan mediante Maven. Aquí están las principales dependencias:

Apache Spark Core: org.apache.spark:spark-core_2.12

Apache Spark SQL: org.apache.spark:spark-sql_2.12

Scala Library: org.scala-lang:scala-library

## Contribuciones
Si deseas contribuir a este proyecto, ¡eres bienvenido! Puedes abrir un issue para reportar errores o sugerir mejoras, o enviar un pull request con tus cambios.
