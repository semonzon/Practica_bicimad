# -*- coding: utf-8 -*-
"""
Created on Sat May  6 20:36:13 2023

@author: USUARIO
"""

'''
        PRÁCTICA BICIMAD
Para esta práctica, es necesario que los archivos estén en una carpeta 
llamada files, y esa carpeta esté donde se encuentre este programa. 
Dentro de la carpeta files se tienen que encontrar los archivos 
de BiciMad, tantos archivos como se considere.
'''

from pyspark import SparkContext,SparkConf
import os,json
from numpy import mean

# Función para obtener los datos de cada linea
def obtener_datos(linea):
    datos = json.loads(linea)
    tiempo_viaje = datos["travel_time"]
    origen = datos["idunplug_station"]
    destino = datos["idplug_station"]
    return tiempo_viaje, origen, destino

#Función que transforma cada linea
def transformar(lista):
    # Los datos de entrada son de la forma 
    # (estacion1,estacion2,tiempo).
    
    # Primero obtenemos 2 listas. La primera con las estaciones que 
    # son origen de alguna estación y la segunda con las que son destino
    lista_origen = lista.map(lambda x: (x[1] , (x[2],x[0],'origen') ))
    lista_destino = lista.map(lambda x: ( x[2] , (x[1],x[0],'destino') ))
    #print(lista_origen.collext()[0])
    #print(lista_destino.collect()[0])
    
    #Juntamos ambas listas
    lista_final = lista_origen + lista_destino
    
    # Esto nos da una lista en la cual cada elemento tiene como Key
    # la estación y los Values son las estaciónes que conectan 
    # con Key y si son origen o destino
    lista_final2 = lista_final.groupByKey().mapValues(list)
    #print(lista_final2.collect()[0])
    
    
    # Por cada estación, vemos cuantas conexiones tiene, y lo ponemos en la tupla
    # junto a las estaciones que se conecta
    lista_final3 = lista_final2.map(lambda x : (x[0],[len(x[1]), x[1] ] ))
    #print(lista_final3.collect()[0])
    
    # Ordenamos la lista anterior por el número de conexiones, de mayor a menor
    lista_final4 = lista_final3.sortBy(lambda x : x[1][0],ascending=False)
    #print(lista_final4.collect()[0])
    
    return lista_final4
    
def transformar2(lista):
    lista_origen = lista.map( lambda x: (x[1] , x[0]) )
    lista_o2 = lista_origen.groupByKey().mapValues(list).mapValues(mean)
        
    lista_destino = lista.map(lambda x: (x[2] , x[0]) )
    lista_d2 = lista_destino.groupByKey().mapValues(list).mapValues(mean)
    
    lista2 = lista_o2.join(lista_d2)
    #print(lista2.collect())
    lista3 = lista2.groupByKey().mapValues(list)
    #print(lista3.collect())

    lista4 = lista3.sortBy(lambda x: ((x[1][0][0]+x[1][0][1])/2), ascending=False)
    return lista4
    

# Función que dada una linea con la forma 
# (estación, [cantidad_conexiones, L]), donde L es una lista con las estaciones junto
# a su tiempo medio y si son origen o destino
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
    return ( (estacion_principal, cantidad), (origenes, destinos) )
            

# Función que extrae los tiemplos de cada estación 
def obtener_tiempo(tupla):
    tiempos_o = 0
    tiempos_d = 0
    tiempo_origen = tupla[0]
    tiempo_destino = tupla[1]
    for i in tiempo_origen:
        tiempos_o +=i[1]
    tiempos_o = tiempos_o / len(tiempo_origen)
    for i in tiempo_destino:
        tiempos_d += i[1]
    tiempos_d = tiempos_d / len(tiempo_destino)
    return (tiempos_o,tiempos_d)

#Función principal del programa
def main():
    conf = SparkConf().setAppName("Bicimad")
    sc = SparkContext(conf = conf)
    
    #files es la carpeta donde se encuentran los archivos de BiciMad
    files = os.path.abspath('files')
    
    #Por cada archivo de la carpeta
    for filename in os.listdir(files):
        
        #Primero leemos el archivo
        rdd0 = sc.textFile(files+'/'+filename)
        
        # Obtenemos los datos que nos interesan. Para ello utilizaremos la función obtener_datos.
        # rdd1 va a consistir en un rdd con los elementos siendo tuplas del estilo (tiempo_viaje, origen, destino)
        rdd1 = rdd0.map(obtener_datos)
        #print(rdd1.collect()[0])
        
        # Transformamos los datos con la función transformar. Para más explicación consultar la función transformar. 
        rdd2 = transformar(rdd1)
        #print(rdd2.collect()[0])
        
        # Esta función nos da el rdd0 ordenado por tiempo de uso de cada estación. 
        # Los elementos tienen la forma (estación, (tiempo_desde_estación,tiempo_hasta_estación) )
        print('Las 10 estaciones con mas tiempo de uso por bicicleta son: ')
        rdd22 = transformar2(rdd1)
        print(rdd22.take(10))
        print('\n')
        
        
        # Los elementos de rdd2 son tuplas consistentes en (estación_principal, cantidad_de_conexiones, L ), donde L es una
        # lista en la que los elementos son de la forma (estación_secundaria, tiempo_de_viaje_entre_estaciones, origen/destino )
        # Por lo tanto, para cada elemento de rdd2, separamos las estaciones que son origen y las que son destino.
        # En los values, la primera lista son los origen y la segunda los destino
        rdd3 = rdd2.map(separar)
        #print(rdd3.collect()[0])
        
        # Obtenemos los elementos que son tiempos_de_viaje, para luego hacer la media
        rdd4 = rdd3.mapValues(obtener_tiempo)
        rdd5 = rdd4.mapValues(mean)
        #print(rdd4.collect()[0])
        #print(rdd4.collectAsMap()) lo devuelve como si fuese un diccionario
        #print(rdd5.collect()[0])
        
        
        # Obtenemos las estaciones con más cantidad_de_conexiones
        rdd_final = rdd5.take(10)
        print('Las 10 estaciones con mas conexiones son: ')
        print(rdd_final)
        print('\n')
    sc.stop()
    
    


if __name__ == '__main__':
    main()
    
    
