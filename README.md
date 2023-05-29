Programación Paralela: Práctica 4

""" Práctica Bicimad """

Idioma: Python

Creadores:

    Joel Gómez Santos
    
    Sergio Monzon Garces
    
    Celeste Rhodes Rodríguez

1. Motivación

El objetivo de la práctica es analizar los datos de viajes de bicimad para resolver el problema descrito en el documento de la memoria; que estaciones son aquellas con mas flujo de bicicletas y, por tanto, donde merece la pena realizar reparaciones.

2. Descripción

El programa primero crea una sesión de Spark y recoge y abre los datos de los archivos de información de tipo json que deseamos analizar.

Analizamos cuáles son las estaciones con más tiempo de uso por bicicleta, es decir, cuánto tiempo pasa desde que se detecta que la bicicleta sale de la estación hasta que llega a otra estación dividido entre el número de viajes. Nos quedamos con las diez primeras y las representamos en un dataframe.

A continuación calculamos cuales son las estaciones con más viajes cuyo comienzo o destino sea dicha estación y nos quedamos con el top 10. Las representamos en una tabla dataframe junto a su número de conexiones.

Durante el programa analizamos el tiempo que tardan las funciones en ejecutarse para poder comparar su eficiencia cuando analizan distintos archivos.

3. Modo de uso

Para ejecutar el programa el usuario debe instalar los módulos pyspark, os, json, numpy y time, los cuales pueden instalarse en el ordenador fácilmente.

El usuario que desee ejecutar el programa debe tener los archvos de los datos .json que desee analizar o, en su defecto, estar conectado a un cluster donde se encuentren estos archivos.

Basta ejecutar los programas y este presentará los datos de interés en formato dataframe.

El archivo .py definitivo a ejecutar es el que llamamos 'practica_final.py'.
