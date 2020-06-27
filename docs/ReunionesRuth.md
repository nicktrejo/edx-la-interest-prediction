# TFM TREJO

# Servicio web para la predicción de interés de los estudiantes en cursar MOOCs en el itinerario verificado

* sistema web (web service)!! cualquiera pueda usarlo:

  * administrador
  * docente
  * alumno

---

mencionar el trabajo previo [de Ruth]

citar herramientas previas

python + dash + flask (R)
...

DATOS
todos menos los ejercicios.
navegacion, ver vídeos, 
patrones
reconocimiento de patrones


---

## Reunion 25/03

OBJETIVO: verbo en definitivo (implementar, diseñar, escribir la memoria del fin de master)


estado del arte

diseño

implementacion
desarrollo

evaluacion y resultados


BIBLIOGRAFIA
- corregir
- usar el estilo que me da ruth(primer comentario)

para mas detalle el objetivo (buscar palabra para subojetivos en )


Estado del arte en herramientas de LA
Análisis de datasets provenientes de edX
Análisis y entendimiento de módulos LA existentes, pricipalmente en cuando a su función y su arquitectura

Análisis de datos procesados por módulo existente
Evaluación de la posible mejora y/o adaptación del módulo de procesamiento
Análisis descriptivo de los datos útiles previo a las predicciones




---

## Reunion 02/04

TFG de Lara Olmo

Carpeta INVESTIGACION de OneDrive

ruth direcciones (están los TFG y TFM)


---

## Reunion 13/04

### FECHAS:
* primer evento del usuario (audit)
* fecha de cambio de modo a verified --> 'edx.course.enrollment.mode_changed'
* fecha de cert -->  (alternativa: del fichero cert.csv)
* fecha de ultimo evento -->

* *date-audit* : día en que comenzó a cursar, coincide con el día *edxmongo.final\_indicators.connected\_days* pasa a ser 1
* *date-verified* : día en que comenzó a cursar, se obtiene con el evento 
* *date-certified* :  (alternativa: del fichero cert.csv)
* *date-end* : 


### DOCUMENTACION
* edX
* codigo

* cada usuario es una fila
  * cada columna es una fecha

### MONGO
* persona
  * indicador
    * dia

* persona
  * fecha
    - cada fichero es un día

MONGO preguntar a Juan Soberon

*TODO*: Leer TFG de Lara Olmo y de Victor Macias

---

## Reunion 26/04

* Encuesta a la mitad del curso para saber su interés, si lo quieren terminar y si pagarían y por qué (con escala likert)


2 entradas:
 1. logs (+ mongo)
 2. survey processing (respuestas a preguntas explícitas)

---

## Pendientes al 25/07

1. Conseguir código del Dash (mail a Juan S)
1. Conseguir últimos logs
1. Agregar a todos los alumnos (verificados y *no verificados*)
1. Agregar lectura de certified
1. Leer datos de la BD (Mongo)
1. Fecha de encuensta intermedia (pedir a Sorin)
1. Gráficos del notebook x semana

