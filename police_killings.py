import plotly.offline as py
import plotly.graph_objects as go
from pyspark import SparkContext
sc = SparkContext("local","Practica1")
path1 = "C:\\Users\\Bayyron\\Desktop\\Junio2020\\Seminario2\\Laboratorio\\Archivos\\Libro3.csv"
texto = sc.textFile(path1)
Rdd = texto.map(lambda linea:linea.split('|'))\
    .filter(lambda linea:(linea[3] != ""))\
    .filter(lambda linea:(linea[3] != "Victim's race"))\
    .map(lambda linea:(linea[3].upper(),1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda linea: linea[1], ascending=False)

print("EJERCICIO1")
TOP = Rdd.take(3)
Ejex = []
EjeY = []
for datos in TOP:
    Ejex.append((datos[0]))
    EjeY.append((datos[1]))
    print("Raza->" + str(datos[0]) + "  Total Victimas->" + str(datos[1]))

GraficaBarras = go.Bar(
    x = Ejex,
    y = EjeY,
    text=EjeY,
    textposition='auto'
)

data = [GraficaBarras]
py.plot(data,filename="police_killings_1.html")
Rdd = texto.map(lambda linea:linea.split('|'))\
    .filter(lambda linea:(linea[5] != ""))\
    .filter(lambda linea:(linea[3] != "Victim's race"))\
    .filter(lambda linea:(len(linea[5]) == 10))\
    .map(lambda linea:(linea[5].split("/")[2],1))\
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda linea: linea[1], ascending=False)

print("EJERCICIO2")
TOP = Rdd.take(5)
Ejex = []
EjeY = []
for datos in TOP:
    Ejex.append(("Anio-" + str(datos[0])))
    EjeY.append((datos[1]))
    print("Anio->" + str(datos[0]) + "  Total casos->" + str(datos[1]))

GraficaBarras = go.Bar(
    x = Ejex,
    y = EjeY,
    text=EjeY,
    textposition='auto'
)
data = [GraficaBarras]
py.plot(data,filename="police_killings_2.html")