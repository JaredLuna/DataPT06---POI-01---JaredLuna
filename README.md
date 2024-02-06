# DataPT06---POI-01---JaredLuna

Jared Augusto Luna León
DataScience DataPT06

Este es el repositorio del primer proyecto individual de Henrey.

En este proyecto se nos planteó la problemática de desarrollar un MVP, el cual constaba en realizar el análisis de bases de datos de la plataforma STEAM. Esto con el fin de desarrollar unas funciones que serán explicadas más adelante.  Se planteó realizar una EDA, opcionalmente una ETL, luego programar las funciones en Python y finalmente, realizar una API que mediante un WebService (en este caso Render) pueda ser consultada por diversos usuarios.

EDA:
Para la parte de la EDA primeramente tuvimos que hacer un acomodo de nuestros archivos, ya que se nos entregaron archivos .json comprimidos con gzip. Al estar realizando la EDA nos dimos cuenta de que unas columnas contaban con información anidada y se tuvo que retrabajar para poder extraer esa información y colocarla en otro archivo para su futura manipulación. 
En esta parte también se pudo observar que teníamos datos faltantes o nulos y columnas que no nos iban a servir para el análisis, como lo eran columnas de URL’s o de nombres repetidos. Todos estos insights fueron apuntados para posteriormente en el paso de la ETL transformar nuestros datasets.

ETL:
Para esta parte no se necesitó gran trabajo, el proyecto en sus instrucciones nos planteo que esta parte era opcional, sin embargo, se opto por hacer una limpieza rápida para tener un mejor manejo de nuestros datos, ya que estábamos manejando unos archivos csv bastante pesados.
Como se comentó en la parte de la EDA ya se tenían los elementos que había que eliminar, los cuales fueron columnas innecesarias con URL’s, teníamos columnas con datos repetidos que no nos iban a aportar en nuestro análisis, por lo tanto, también se optó por eliminarlas. Luego se habían importado los archivos en formato CSV, pero se tuvo que transformar al final a un formato parquet porque si no era imposible tener nuestros datos en GitHub. Es por eso que casi todo el código esta importando como CSV, pero al final se termina trabajando con archivos parquet.

Funciones:
Para las funciones todo el código esta comentado explicando paso por paso que es lo que se fue haciendo, pero prácticamente se estuvo trabajando solamente con Python y pandas. No se utilizó SQL para hacer los filtros ni queries. Se fueron extrayendo las columnas que deseábamos, extrayendo los ID’s necesario. Lamentablemente para la última función no hubo tiempo para realizar su desarrollo, pero más adelante se va a continuar con el mismo. 
En esta parte para la lectura de los datos como no se tenía conocimiento de como era generar API’s etc, se hace la lectura de los datos en cada función, pero se puede lograr un empaquetamiento mejor jalando solamente una vez los datos y dependiendo de la función se importará solamente el dataframe necesario para poder tener un código más eficiente.
Para la función de conseguir las horas más jugadas por años se tuvo una duda, ya que en uno de los dataframes existían dos columnas, una que nos indicaba el “playtime forever” que se tomó como el tiempo de juego desde que el juego fue lanzado al mercado, por lo tanto, esa columna nunca se utilizó para el análisis. La columna que se tomó fue la de “playtime_2week”. Ya que se cree que es el tiempo de juego dos semanas después de su lanzamiento, así que como se mantiene dentro del margen del mismo año pues para todos se tomó ese tiempo y así generalizamos.
Para las funciones 3 y 4 relativamente es el mismo código ya que se obtiene un dataframe con el nombre de los juegos y la cantidad de usuarios que lo recomiendan. Entonces se ordenan de mayor a menor y para la función 3 se toman los primeros 3 lugares y para la función 4 se toman los últimos 3 lugares, con eso obtenemos el top 3 de los juegos más recomendados y el top 3 de los juegos menos recomendados.

API:
En esta parte primero se simulo de manera local el deploy de la API mediante FASTAPI. Se creó un entorno virtual, se instalaron librerías, se copiaron los datos y se genero un nuevo main.py con las funciones y sus retornos listos para entregarlos a la API. Una vez terminado esto se hizo el deploy en Render, el cual es bastante sencillo con uvicorn. Solamente se creo el repositorio de nuestro proyecto en GitHub, se subió ahí y desde render se jala el proyecto de GitHub haciendo todo muy amigable y rápido, ya que con cualquier commit que se realice al repo, la API toma la actualización en tiempo real y así la API siempre puede estar actualizada a la última versión.

Conclusiones:
Al final el proyecto puede ser un MVP para lo que fue solicitado, necesita ser pulido ya que por cuestiones laborales el tiempo que se utilizó para el desarrollo de este proyecto no es el que me hubiese gustado. Se seguirá trabajando en este proyecto como práctica para poder tener un proyecto más fluido, más ordenado y al final más completo. Sin embargo, los conocimientos adquiridos fueron de gran ayuda, se aprendió a trabajar con archivos .json, con información anidada, a trabajar con pandas de una manera más útil, el tratamiento de información, el trabajar con archivos parquet y sobre todo el adquirir el conocimiento de como levantar y dar deploy de una API es sumamente valioso. Se pensaba que era algo más complejo o difícil, pero no lo es, solamente que no se tenía el conocimiento y ahora es más fácil seguir una metodología de trabajo cuando se vaya a levantar una API.



