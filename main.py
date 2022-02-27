#Lo primero que vamos a hacer es importar todas las librerias necesarias 
#para llevar a cabo el proyecto


import requests
import pandas as pd
import os
import csv
import datetime
import time
import numpy as np
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
from decouple import config


#Creamos las carpetas donde se almacenaran los datos previos a su limpieza:

os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\')

if os.path.exists('DataCultura'):
    print("Carpeta existente")
else: os.mkdir('DataCultura')
 
#modifico el path: 
os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\DataCultura')


#Creo las subcarpetas: 

# "museos"
if os.path.exists('museos'):
    print("Carpeta museos creada")
else: os.mkdir('museos')

#"SalasDeCine"
if os.path.exists('SalasDeCine'):
    print("Carpeta SalasDeCine creada")
else: os.mkdir('SalasDeCine')

#"BibliotecasPopulares"
if os.path.exists('BibliotecasPopulares'):
    print("Carpeta BibliotecasPopulares creada")
else: os.mkdir('BibliotecasPopulares')



#Dentro de la categoria 'museos' creo la carpeta 'Año-Mes' si no existe

os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\DataCultura\\museos')

fecha = datetime.today()          # nos da la fecha de hoy
añoMes = fecha.strftime("%Y-%B")  # nos da el Año-Mes actual en formato string

#si NO existe el fichero "añoMes" --> creo el fichero

if os.path.exists(añoMes):
    print("Archivo existente")
else: os.mkdir(añoMes)

os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\DataCultura\\museos\\'+añoMes)

#Obtengo los datos de museos:

# Hago un get para obtener los datos
museos = requests.get('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos.csv')

#Obtengo la fecha del dia en el formato que me interesa
fechaHoy = datetime.today()
fechaCarga = fechaHoy.strftime("%d-%m-%Y")

# Guardo los datos obtenidos en un archivo CSV con la fecha actual
f = open ('museos-'+fechaCarga+'.csv','w', encoding='utf-8')
f.write(museos.text)
f.close()

#Una vez hecho esto puedo reutilizar mi codigo con los datos de 'salas de cine' y 'bibliotecas populares'


#SALAS DE CINE 

os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\DataCultura\\SalasDeCine')

fecha = datetime.today()    # nos da la fecha de hoy
añoMes = fecha.strftime("%Y-%B")  # nos da el Año-Mes actual en formato string

#si NO existe el fichero "añoMes" --> creo el fichero

if os.path.exists(añoMes):
    print("Archivo existente")
else: os.mkdir(añoMes)

os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\DataCultura\\SalasDeCine\\'+añoMes)

# Hago un get para obtener los datos de salas de cine
salasDeCine = requests.get('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv')

#Obtengo la fecha del dia en el formato que me interesa
fechaHoy = datetime.today()
fechaCarga = fechaHoy.strftime("%d-%m-%Y")

# Guardo los datos obtenidos en un archivo CSV con la fecha actual
f = open ('salasDeCine-'+fechaCarga+'.csv','w', encoding='utf-8')
f.write(salasDeCine.text)
f.close()



#Bibliotecas populares

os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\DataCultura\\BibliotecasPopulares')

fecha = datetime.today()    # nos da la fecha de hoy
añoMes = fecha.strftime("%Y-%B")  # nos da el Año-Mes actual en formato string

#si NO existe el fichero "añoMes" --> creo el fichero

if os.path.exists(añoMes):
    print("Archivo existente")
else: os.mkdir(añoMes)

os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\DataCultura\\BibliotecasPopulares\\'+añoMes)
os.getcwd()


# Hago un get para obtener los datos de bibliotecas populares
bibliosPopulares = requests.get('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv')

#Obtengo la fecha del dia en el formato que me interesa
fechaHoy = datetime.today()
fechaCarga = fechaHoy.strftime("%d-%m-%Y")

# Guardo los datos obtenidos en un archivo CSV con la fecha actual
f = open ('bibliotecasPopulares-'+fechaCarga+'.csv','w', encoding='utf-8')
f.write(bibliosPopulares.text)
f.close()


# Una vez obtenidos los datos y guardados en formato CSV los metemos dataframes de pandas 
# para trabajar los datos


#Datos de Museos

os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\DataCultura\\museos\\'+añoMes)

#cargo los datos a un DF para limpiarlos
dfM = pd.read_csv('museos-'+fechaCarga+'.csv', encoding='utf8')
#dropeo las columnas que no me van a servir
dfM.drop(['subcategoria', 'piso','Observaciones','Latitud','Longitud','TipoLatitudLongitud','Info_adicional','jurisdiccion','aÃ±o_inauguracion','IDSInCA'], axis=1, inplace=True)
#renombro las columnas de acuerdo a lo pedido
dfM.rename(columns={'IdDepartamento':'id_departamento','Cod_Loc':'cod_localidad','IdProvincia':'id_provincia','direccion':'domicilio','CP':'codigo postal'}, inplace=True)





#Datos de Salas de cine
os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\DataCultura\\SalasDeCine\\'+añoMes)

#cargo los datos en un dataframe dfC y luego en otro dfCines que usare luego:
dfC = pd.read_csv('SalasDeCine-'+fechaCarga+'.csv', encoding='utf8')
dfCines = pd.read_csv('SalasDeCine-'+fechaCarga+'.csv', encoding='utf8')

#De nuevo dropeo las columnas que no me van a servir
dfC.drop(['Observaciones','Departamento','Piso','InformaciÃ³n adicional','Latitud','Longitud','TipoLatitudLongitud','tipo_gestion','Pantallas','Butacas','espacio_INCAA','aÃ±o_actualizacion'], axis=1, inplace=True)

#y ahora las renombro

dfC.rename(columns={'Fuente':'fuente','TelÃ©fono':'telefono','Nombre':'nombre','Localidad':'localidad','Provincia':'provincia','IdDepartamento':'id_departamento','Cod_Loc':'cod_localidad','IdProvincia':'id_provincia','DirecciÃ³n':'domicilio','CP':'codigo postal','CategorÃ­a':'categoria'}, inplace=True)





#Datos de Bibliotecas Populares
os.chdir('C:\\Users\\Segov\\Desktop\\CulturaData (ETL)\\src\\DataCultura\\BibliotecasPopulares\\'+añoMes)

#Cargo los datos en un dataframe
dfB = pd.read_csv('BibliotecasPopulares-'+fechaCarga+'.csv', encoding='utf8')

#dropeo columnas innecesarias
dfB.drop(['Observacion','Subcategoria','Departamento','Piso','InformaciÃ³n adicional','Latitud','Longitud','TipoLatitudLongitud','Tipo_gestion','aÃ±o_inicio','AÃ±o_actualizacion'], axis=1, inplace=True)

#renombro las columnas
dfB.rename(columns={'Fuente':'fuente','Cod_tel':'cod_area','TelÃ©fono':'telefono','Nombre':'nombre','Localidad':'localidad','Provincia':'provincia','IdDepartamento':'id_departamento','Cod_Loc':'cod_localidad','IdProvincia':'id_provincia','Domicilio':'domicilio','CP':'codigo postal','CategorÃ­a':'categoria'}, inplace=True)




#Ahora puedo unir los dataframes en un dataframe general que contenga todos los datos

dfG = pd.concat([dfM, dfC, dfB])

# En la columna categoria de los valores que corresponden al dataframe dfM (museos) remplazo
# "espacio de exibicion patrimonial" por "Museos"
dfG['categoria'].replace('Espacios de ExhibiciÃ³n Patrimonial','Museos',inplace=True)



#Veamos los valores que contiene la columna provincia

    #print(dfG['provincia'].unique())

#Hay mucho que limpiar, los nombres de provincia estan repetidos asi que debo remplazarlos

dfG['provincia'].replace(['Santa FÃ©', 'CÃ³rdoba', 'TucumÃ¡n', 'Entre RÃ\xados', 'RÃ\xado Negro', 'Ciudad AutÃ³noma de Buenos Aires', 'NeuquÃ©nÂ\xa0', 'NeuquÃ©n', 'Tierra del Fuego, AntÃ¡rtida e Islas del AtlÃ¡ntico Sur', 'Ciudad Autónoma de Buenos Aires'] ,['Santa Fe', 'Córdoba', 'Tucumán', 'Entre Ríos', 'Río Negro', 'Ciudad Autónoma de Buenos Aires', 'Neuquen', 'Neuquen', 'Tierra del Fuego', 'Buenos Aires'], inplace=True)

#print(dfG['provincia'].unique())

# veamos si hay mas valores repetidos en las otras columnnas

    #print(dfG['domicilio'].describe())

#se repite "sin direccion" por lo que los voy a remplazar por null

dfG['domicilio'].replace('Sin direcciÃ³n', np.nan ,inplace=True)

#luego de explorar los datos veo que hay muchos valores "s/d" por lo que los remplazo por null

dfG = dfG.replace('s/d', np.nan)

# por ultimo hay que combinar las columnas "cod_area" y "telefono" en una columna "numero de telefono"
# (me parecio bien guardarlo en formato str ya que son numeros con los que no me interesa operar)
dfG['numero de telefono'] = dfG['cod_area'].astype(str) +''+ dfG['telefono'].astype(str)


#con este dataframe dropeamos la columna fuente y ya podemos cargarlo como la tabla general

dfGeneral = dfG.drop(['fuente'],  axis=1)

######### dfGeneral sera nuestra primer tabla en la BD #########



#Ahora analicemos los datos de salas de cine:

dfCines.drop(['Observaciones', 'InformaciÃ³n adicional', 'Latitud', 'Longitud', 'TipoLatitudLongitud', 'tipo_gestion', 'aÃ±o_actualizacion','Piso'], axis=1, inplace=True)
dfCines.rename(columns={'Cod_Loc':'codigo_localidad','IdProvincia':'Id_provincia','IdDepartamento':'Id_departamento','CategorÃ­a':'categoria','DirecciÃ³n':'direccion','CP':'codigo_postal','TelÃ©fono':'telefono'}, inplace=True)

#veo que aparece "si" y "SI" como significan lo mismo voy a unificarlos
dfCines.replace('SI', 'si', inplace = True)
dfCines = dfCines.replace('0', np.nan)

### Ahora analicemos los datos de las salas de cine y obtengamos la cantidad de: pantallas, butacas y espacios INCAA
#   por provincia
#print(dfCines.columns)

dfButacasPantallas = dfCines[['Provincia','Pantallas','Butacas']]

dfINCAA = dfCines[['Provincia','espacio_INCAA']]

#print(dfButacasPantallas.head(15))

dfButacasPantallas = dfButacasPantallas.groupby('Provincia').sum()

#print(dfButacasPantallas)


#print(dfINCAA.unique())

#puedo remplazar 'si' por 1 y hacer la suma agrupados por provincia

dfINCAA = dfINCAA.replace('si', 1)

dfINCAA = dfINCAA.groupby('Provincia').sum()

#print(dfINCAA)

### y ahora los uno en un solo dataframe usando la columna 'Provincia'

dfCinesF = pd.merge(dfButacasPantallas, dfINCAA, on='Provincia')

dfCinesF = dfCinesF.astype(int)

# print(dfCinesF)

########## ya tenemos el dataframe dfCinesF como la tabla que cargaremos a la BD

### ahora analicemos los datos generales 

# Registros totales por categoria

dfTotCat = dfG[['categoria','provincia']]

dfTotCat = dfTotCat.groupby('categoria').count()



#print(dfTotCat.head(10))

# Ahora el total por fuente

dfTotFuente = dfG[['provincia','fuente']]

dfTotFuente = dfTotFuente.groupby('fuente').count()

#print(dfTotFuente.head(10))

#total por categoria y provincia 


dfTotCatProv = dfG.groupby(['provincia','categoria'])

dfTotCatProv = dfTotCatProv[['provincia','categoria']].count()

#print(dfTotCatProv.head(20))

# dfTotCatProv  tiene los registros totales por provincia y categoria

#  Deberia hacer un dataframe dfTotales con todos los registros agrupados

dfTotales = pd.concat([dfTotCatProv, dfTotCat, dfTotFuente])

dfTotales = dfTotales.drop(['categoria'], axis=1)
dfTotales = dfTotales.rename(columns= {'provincia' : 'total'})

#print(dfTotales)

##### Ya tengo las 'tablas' con las que voy a poblar mi base de datos, hora de crear la conexion y cargarlos 
#      con 'pd.to_sql'


dbhost = config('POSTGRES__HOST')
dbuser = config('POSTGRES__USER')
dbpass = config('POSTGRES__PASSWORD')
dbname = config('POSTGRES__DB')
postgresEngine = config('POSTGRES__ENGINE')

engine = create_engine('postgresql+psycopg2://postgres:123456@localhost/DBcultura')

conn = psycopg2.connect(
    host=dbhost,
    database=dbname,
    user=dbuser,
    password=dbpass)


#Podemos reutilizar fechaCarga que usamos para la escritura de los archivos csv
dfGeneral = dfGeneral.assign(FechaCarga = fechaCarga)

#y cargamos el dataframe con la fecha agregada en columna 'FechaCarga'
dfGeneral.to_sql('DatosGenerales', con=engine, if_exists='replace')


#Ahora hacemos lo mismo con los datos de salas de cines

dfCinesF = dfCinesF.assign(FechaCarga = fechaCarga)

dfCinesF.to_sql('DatosCines', con=engine, if_exists='replace')

#Y finalmente el dataframe con los datos por categoria, fuente y provincia y categoria

dfTotales = dfTotales.assign(FechaCarga = fechaCarga)

dfTotales.to_sql('DatosCines', con=engine, if_exists='replace')