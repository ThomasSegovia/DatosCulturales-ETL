# DatosCulturales-ETL

#### Descripcion
 Este es un proyecto el cual consume datos desde 3 fuentes distintas para popular una base de datos SQL con información cultural sobre bibliotecas, museos y salas de cines argentinos.

#
#### Guia de uso
 Para llevar a cabo este proyecto deben llevar a cabo 3 pasos: 
* crear un entorno virtual 
* instalar las librerias necesarias 
* configurar la conexion a la base de datos

#
#### Creacion del entorno virtual:
#####
1) lo primero que necesitamos es la libreria de python venv, abrimos nuestro cmd y ponemos el comando: 'pip install virtualenv'
2) luego creamos una carpeta donde se aloja nuestro proyecto y con el cmd vamos a la direccion de la carpeta 
3) con el cmd apuntando a la carpeta de nuestro proyecto ponemos el comando: 'virtualenv nombre'    -> donde 'nombre' sera el nombre de nuestro entorno virtual
4) Ahora activaremos el entorno virtual, lo haremos con el comando '.\nombre\Scripts\activate' -> donde 'nombre' sera el nombre de nuestro entorno virtual
#####
#
#### Instalacion de las librerias necesarias:
una vez activado nuestro entorno virtual vamos a empezar a insalar librerias con pip
1) primero ponemos el comando 'pip install requests'
2) luego 'pip install pandas'
3) luego 'pip install numpy'
4) luego 'pip install python-decouple python-dotenv'
5) luego 'pip install sqlalchemy'
6) y 'pip install psycopg2'
#
#### Configuracion de la base de datos:
 la configuracion de la base de datos se hace a traves de un archivo .env, se debe crear un archivo especificando:
 
 ##### POSTGRES__HOST= 'tu host'
 ##### POSTGRES__USER= 'tu usuario'
 ##### POSTGRES__PASSWORD= 'tu contraseña'
 ##### POSTGRES__DB= 'el nombre de tu base de datos' 
 
 #
 #
 ### Con estos pasos ya deberia ser posible levantar el proyecto
