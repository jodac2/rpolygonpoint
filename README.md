# RPolygonPoint

[RPolygonPoint](https://github.com/jodac2/rpolygonpoint.git) es una pequeña librería no oficial de python y spark que permite generar puntos aleatorios de un poligono a gran escala y de manera eficiente. Además, permite realizar la construcción de poligonos aleatorios. Parte de la eficiencia de los algoritmos radica en que a excepción de algunas sentencias de control, la libería utiliza únicamente funciones nativas de spark.

## Origen
---

El core de la librería ***RPolygonPoint*** consiste en *determinar si un punto esta dentro de un polígono* dado. Surge como solución a un problema práctico en el que se requiere realizar la identificación de la zona geográfica (digamos municipio para el caso de México) a la que pertenecee una ubicación. Debido a que el volumen de ubicaciones es muy alto (aproximadamente ***seis mil millones***) y a que se requiere tener el resultado en un tiempo considerablemente pequeño, fue necesario desarrollar una solución que se pudiera ejecutar en distribuido. 


## Componentes
---

La librería tiene tres métodos principales

1. `get_container_polygon`: Dado un conjunto de puntos y uno de polígonos, determina cuales de los polígonos son contenederes de los puntos.

2. `get_random_polygon_point`: Dado un polígono y un tamaño de muestra, el método genera un conjunto de puntos aleatorios que pertenecen al polígono.

3. `get_random_polygon`: Dado un número de lados y algunos parámetros de configuración (dependiendo del tipo de simulación) genera un conjuto de polígonos aleatorios.

## Algoritmo: Container Polygon
---

 La ide básica del algoritmo es utilizar el [método del rayo](https://en.wikipedia.org/wiki/Point_in_polygon) para determinar si un punto pertenece o no a un polígono dado. Sin embargo, si el número de polígonos o puntos es relativamente alto el problema se vuelve costoso en tiempo de ejecución, ya que se debe hacer el producto cartesiano entre el junto de puntos y polígonos; y posteriormente aplicar el método del rayo sobre cada una de las posibles combinaciones. Con esto es evidente que se deben utilizar métodos menos costosos que permitan reducir el tiempo de ejecución, la solución que se plantea consiste en los siguientes pasos.


 ### Paso 1: Rectángulo delimitador 
 
Consiste en determinar el rectángulo que delimita a cada uno de los polígonos. Esto permite realizar una *validación extremadamente rápida para determinar si el punto esta fuera del polígono*.  Ya que si el punto se encuentra fuera del rectángulo, entonces el punto no puede estar dentro del polígono.

Si bien este paso permite reducir bastante el tiempo de ejecución, aún no es sufiente para lo requerido. Por ende se busca reducir aún más la tasa de falsos positivos y delimitar algunas regiones interiores del rectángulo delimitador en las que no sea necesarío utilizar el método del rayo para confirmar o descartar la pertenencia del punto al polígono.

### Paso 2: Malla

Dada una resolución, que puede ser diferente para cada polígono, se genera una malla sobre el rectángulo delimitador de cada polígono. Posteriormente se realiza una clásificación de los componentes de la malla en tres tipos

- Tipo ***outside***: está totalmente fuera el polígono.
- Tipo **inside**: está totalmnte contenido en el polígono.
- Tipo ***undecided***: una parte está fuera del polígono y otra está dentro.

### Paso 3: Mosaico

Los Pasos 1 y 2 pueden verse como una especie de pre-proceso si los polígonos no se modifican con el tiempo. El paso final es determinar dentro de cual mosaico de la malla del rectángulo delimitador se encuentra el punto, si es que lo está. Si el punto se encuentra en un mosaico del tipo ***outside*** o ***inside***, entonces automáticamente se puede determinar si el punto está dentro o fuera del polígono. Si el mosaico es de tipo ***undecided***, entonces se debe emplear el método del rayo para decidir.

## Algoritmo: Random Point
---

El algoritmo de simulación de puntos de un poligono tiene la idea básica de un método de [muestro Gibbs](https://en.wikipedia.org/wiki/Gibbs_sampling). Es decir, en lugar de simular de la distribución multivariada completa a la vez, el muestreo Gibbs realiza simulaciones parciales a partir de las ditribuciones condicionales.

## Por hacer
---

Algo interesente que se puede hacer para mejorar aún más el método que se plantea sería establecer un método para seleccionar la resolución de la malla de manera que se redusca el número de mosaicos tipo ***outside*** sin que se vea afeacto el tiempo de ejecución por el número de mosaicos del rectángulo delimitador.



## Licencia
---

[GNU GPL-V2](https://www.gnu.org/licenses/old-licenses/gpl-2.0.txt)
