# -*- coding: utf-8 -*-
"""
Created on Tue May  9 13:58:31 2023

@author: USUARIO
"""


'''

        PRÁCTICA BICIMAD
        
Para esta práctica, es necesario que los archivos que contienen los datos de BiciMad estén en una carpeta 
llamada files, y esa carpeta esté donde se encuentre este programa. Dentro de la carpeta files se tienen que encontrar los archivos 
de BiciMad, tantos archivos como se considere.

Los archivos de BiciMad tienen los datos organizados de la siguiente manera: 
    { "_id" : { "$oid" : "5a505acd2f384319304ed614" }, "user_day_code" : "b00665a845be18ed9f036c2d72def1ef2fa2c49365cb64644821b72d560b100f", "idplug_base" : 1, "user_type" : 1, "idunplug_base" : 14, "travel_time" : 284, "idunplug_station" : 6, "ageRange" : 5, "idplug_station" : 7, "unplug_hourTime" : { "$date" : "2018-01-01T00:00:00.000+0100" }, "zip_code" : "28010" }
Solo nos van a interesar los datos 'travel_time', 'idplug_station' y 'idunplug_station'

En cada función, se intentará detallar con la mayor claridad posible lo que realice la misma. 

'''

from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import os,json
from numpy import mean

'''
Obtener_datos
Esta función toma de entrada una linea como la que se ha puesto de ejemplo en la introducción de este programa. 
Su objetivo es obtener los datos necesarios para luego el tratamiento de estos datos seleccionados. La función
obtiene los datos 'travel_time', 'idplug_station' y 'idunplug_station'.

'''
def obtener_datos(linea):
    
    datos = json.loads(linea)
    tiempo_viaje = datos["travel_time"]
    origen = datos["idunplug_station"]
    destino = datos["idplug_station"]
    
    return tiempo_viaje, origen, destino

'''
Transformar_conexiones
Esta función toma de entrada un rdd donde los elementos del rdd son de la forma (T,A,B) = (tiempo_viaje,origen,destino). Esto representa 
cada viaje del origen A al destino B, tardando un tiempo T. 
Los pasos que realiza la función son los siguientes:
    1. Primero hace un map del rdd de entrada, en el cual se obtiene:
        1.1 Primero se obtiene un rdd donde los elementos son (A,B,T,'origen').
        1.2 Después se obtiene un rdd donde los elementos son (B,A,T,'destino')
        Esto se hace para que cada estación A sea 'origen' de una estación B y también la estación A sea 'destino' de la estación B.
        La finalidad es luego separar los tiempos en los cuales A es origen y destino, y así hacer un estudio mas detallado de cada estación. 
        1.3 Se unen ambos rdd's
    2. Mas adelante, este rdd:
        2.1 Se agrupa por los Key (en este caso se agrupan por cada estación)
        2.2 Se observa cuantas conexiones (es decir, en len de la lista que tiene por valores)
        2.3 Se ordenan por las conexiones

La salida es un rdd donde sus elementos son de la forma (A,(L,X))
donde A es la estación principal, L son sus conexiones y X son los elementos de la forma (B,T,'origen','destino'), 
donde B es la estacion conectada con A, T el tiempo, y 'origen'/'destino' dependiendo de lo que sea A de B
'''
def transformar_conexiones(rdd0):
    
    rdd1 = rdd0.\
           map(lambda x: (x[1] , (x[2],round(x[0]),'origen') )) + rdd0.map(lambda x: ( x[2] , (x[1],round(x[0]),'destino') ))
    #      \---------------------1.1--------------------------\1.3\------------------------1.2------------------------------\ 
    rdd_final = rdd1.\
                groupByKey().\
                mapValues(list).\
                map(lambda x : (x[0],[len(x[1]), x[1] ] )).\
                sortBy(lambda x : x[1][0],ascending=False)
    # rdd_final = rdd1.\
    #             2.1 .\ 
    #             2.1 .\
    #             2.2 .\
    #             2.3
    
    return rdd_final

'''
transformar_tiempo
Esta función toma de entrada un rdd donde los elementos del rdd son de la forma (T,A,B) = (tiempo_viaje,origen,destino). Esto representa 
cada viaje del origen A al destino B, tardando un tiempo T. 
Los pasos que realiza la función son los siguientes:
    1. Primero se obtiene un rdd donde :
        1.1 Primero se hace un map para que los elementos tengan la forma (A,T)
        1.2 Después se agrupan los datos por las estaciones A y se hace la media de los tiempos. 
        Esto se hace para obtener, para cada estación A, la media de todos los viajes que se realizan DESDE A. 
        1.3 y 1.4 Es análogo a 1.1 y 1.2 pero con las estaciones HASTA A. 
    2. Después se crea otro rdd:
        2.1 Se juntan los rdd anteriores
        2.2 Se agrupan por estaciones
        2.3 Se ordena en función del tiempo medio, el cual es (tiempo_origen + tiempo_destino) / 2
La salida es este último rdd. 
La finalidad de esto es para luego poder ordenar los elementos en función de su tiempo medio, y asi ver que estaciones tienen las bicicletas
con mas tiempo de uso  

'''

def transformar_tiempo(rdd0):
    
    rdd_origen = rdd0.\
                    map( lambda x: (x[1] , x[0]) ).\
                    groupByKey().mapValues(list).mapValues(mean).mapValues(round)
        
    rdd_destino = rdd0.map(lambda x: (x[2] , x[0]) ).\
                    groupByKey().mapValues(list).mapValues(mean).mapValues(round)
    
    rdd_final = rdd_origen.join(rdd_destino).\
             groupByKey().mapValues(list).\
             sortBy(lambda x: ((x[1][0][0]+x[1][0][1])/2), ascending=False)
    
    return rdd_final
    

'''
separar 
Esta función tiene como entrada la salida de la funcion transformar_conexiones.
El objetivo de esta función separar los elementos que son origen de los que son destino, para así
poder ya manipularlos por separado. 
Para ello, por cada elemento de values, ve si es origen o destino y lo introduce en la lista correspondiente.
'''
def separar(linea):
    
    estacion_principal = linea[0]
    resto = linea[1]
    cantidad = resto[0]
    estaciones_secundarias = resto[1]
    origenes = []
    destinos = []
    
    for estacion in estaciones_secundarias:
        if estacion[2] == 'origen':
            origenes.append((estacion[0],estacion[1]))
        elif estacion[2] == 'destino':
            destinos.append((estacion[0],estacion[1]))
            
    return ( estacion_principal, (cantidad, origenes, destinos) )
            

'''
obtener_tiempo
Esta función tiene como entrada la salida los values de la función separar. 
Su objetivo es obtener la media de los tiempos de origen y la media de los tiempos de destino, y devolverlos como salida
''' 
def obtener_tiempo(tupla):
    
    tiempos_o = 0
    tiempos_d = 0
    cantidad = tupla[0]
    tiempo_origen = tupla[1]
    tiempo_destino = tupla[2]
    
    for i in tiempo_origen:
        tiempos_o +=i[1]
    tiempos_o = round(tiempos_o / len(tiempo_origen))
    for i in tiempo_destino:
        tiempos_d += i[1]
    tiempos_d = round(tiempos_d / len(tiempo_destino))
    
    return (cantidad, tiempos_o,tiempos_d)



'''
juntar_tiempos y juntar_conexiones
El objetivo es, como indica el nombre, juntar todos los tiempos de cada estació (separados por origen y destino)
y devolver la media de ambos. La otra función es análoga. 
'''

def juntar_tiempos(lista):
    lista_origen = round(mean([x[0] for x in lista]))
    lista_destino = round(mean([x[1] for x in lista]))
    return lista_origen,lista_destino
def juntar_conexiones(lista):
    lista_final = (round(mean([x[0] for x in lista])),round(mean([x[1] for x in lista])))    
    return lista_final


'''
Función principal del programa
'''
def main():
    '''
    Primero creamos el sc y un spark (para luego poder representar los datos en un DataFrame)
    '''
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    
    '''
    Files es la carpeta donde se encuentran los archivos. rdd_conexiones y rdd_tiempo son los rdd's en donde van
    a ir los correspondientes datos de todos los meses. Es decir, una vez que obtengamos los elementos que nos interesen
    de cada mes, los introducimos en estos rdd's
    '''
    files = os.path.abspath('files')
    rdd_conexiones = sc.emptyRDD()
    rdd_tiempo = sc.emptyRDD()
 
    '''
    Bucle para manipular todos los archivos
    '''
    for filename in os.listdir(files):
      if filename.endswith('.json') :
        '''
        Primero leemos el archivo y obtenemos sus datos
        '''
        rdd_file = sc.textFile(files+'/'+filename).\
               map(obtener_datos)
        
        '''
        rdd_tiempo_file va a obtener los datos de la saluda de la función transformar_tiempo. Es decir, dados unos datos, los elementos
        del rdd van a ser de la forma estación, media_tiempo_origen, media_tiempo_destino, nombre_archivo)
        '''
        rdd_tiempo_file = transformar_tiempo(rdd_file).map(lambda x: (x[0],x[1][0][0],x[1][0][1],filename[:6]))
        print('\n')
        print('Las 10 estaciones con mas tiempo de uso por bicicleta en ',filename[:6],'son: ')
        #print(rdd_tiempo_file.take(10))
        print('\n')
        '''
        Lo representamos en una tabla (formato DataFrame) para una mejor visualización
        '''
        columnas_tiempo = ["Estación","Tiempo_Origen","Tiempo_Destino","Mes"]
        rdd_tiempo_file2 = rdd_tiempo_file.toDF(columnas_tiempo)
        rdd_tiempo_file2.show(10)
        
        '''
        Lo añadimos a rdd_tiempo
        '''
        rdd_tiempo += rdd_tiempo_file
        
        '''
        rdd_conexiones_file va a contener los datos referentes a las conexiones de cada estación. 
        Sus elementos son de la forma (estación, numero_conexiones, tiempo_medio, nombre_archivo)
        El rdd va a estar ordenado gracias a la función sortBy
        '''
        rdd_conexiones_file = transformar_conexiones(rdd_file).\
                               map(separar).\
                               mapValues(obtener_tiempo).\
                               mapValues(lambda x: (x[0],(x[1]+x[2])/2)).\
                               sortBy(lambda x: x[1][0],ascending = False).\
                               map(lambda x: ( x[0],x[1][0],x[1][1],filename[:6] ) )
               
        '''
        Lo añadimos a rdd_conexiones
        '''
        rdd_conexiones += rdd_conexiones_file
        print('\n')
        print('Las 10 estaciones con mas conexiones en', filename[:6],' son: ')
        #print(rdd_conexiones_file.take(10))
        print('\n')
        '''
        Lo representamos en una tabla (formato DataFrame) para una mejor visualización
        '''
        columnas_conexiones = ["Estación","Numero_Conexiones","Tiempo_Medio","Mes"]
        rdd_conexiones_file2 = rdd_conexiones_file.toDF(columnas_conexiones)
        rdd_conexiones_file2.show(10)
        
    '''
    rdd_conexiones va a tener los datos relacionados con el número de conexiones de cada estación de todos los meses
    Sus elementos van a ser de la forma (estación, numero_conexiones, tiempo_medio)
    El rdd va a estar ordenado gracias a la función sortBy
    '''
    rdd_conexiones = rdd_conexiones.\
                     map(lambda x : (x[0],(x[1],x[2],x[3]))).\
                     groupByKey().\
                     mapValues(list).\
                     map(lambda x: (x[0],juntar_conexiones(x[1])) ).\
                     sortBy(lambda x: x[1][0], ascending = False).\
                     map(lambda x: (x[0],x[1][0],x[1][1]))
    
    print('\n')                 
    print('TOTAL CONEXIONES')
    #print(rdd_conexiones.take(10))
    print('\n')
    '''
    Lo representamos en una tabla (formato DataFrame) para una mejor visualización
    '''
    columnas_conexiones = ["Estación","Numero_conexiones","Tiempo_Medio"]
    rdd_conexiones2 = rdd_conexiones.toDF()
    rdd_conexiones2.show(20)
    '''
    rdd_tiempo va a tener los datos relacionados con el tiempo de origen y destino de cada estación de todos los meses
    Sus elementos van a ser de la forma (estación, tiempo_origen, tiempo_destino)
    El rdd va a estar ordenado gracias a la función sortBy
    '''
    rdd_tiempo = rdd_tiempo.\
                 map(lambda x: (x[0], (x[1],x[2])) ).\
                 groupByKey().\
                 mapValues(list).\
                 map( lambda x: (x[0], juntar_tiempos(x[1])) ).\
                 sortBy(lambda x: ((x[1][0]+x[1][1])/2), ascending=False ).\
                 map(lambda x: (x[0],x[1][0],x[1][1]))
                 
    print('\n')
    print('TOTAL TIEMPO')
    #print(rdd_tiempo.take(10))
    print('\n')
    '''
    Lo representamos en una tabla (formato DataFrame) para una mejor visualización
    '''
    columnas_tiempo = ["Estación","Tiempo_Origen","Tiempo_Destino"]
    rdd_tiempo2 = rdd_tiempo.toDF()
    print('\n')
    
    rdd_tiempo2.show(20)
    sc.stop()
    
    


if __name__ == '__main__':
    main()
    
