import plotly.offline as py
import plotly.graph_objects as go
from pyspark import SparkContext
sc = SparkContext("local","Practica1")
path1 = "C:\\Users\\Bayyron\\Desktop\\Junio2020\\Seminario2\\Laboratorio\\Archivos\\Libro2.csv"
texto = sc.textFile(path1)
Rdd = texto.map(lambda linea:linea.split('|'))\
    .filter(lambda linea:(linea[0] != ""))\
    .filter(lambda linea:(linea[0] != "Region"))\
    .map(lambda linea:(linea[0],float(linea[11])))\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda linea: linea[1],ascending=False)

Ejex = []
EjeY = []
print("EJERCICIO1")
for datos in Rdd.collect():
    Ejex.append(datos[0])
    EjeY.append((datos[1]))
    print("Region->" + datos[0] + "  Total->" + str(datos[1]))

GraficaPie = go.Pie(
    labels= Ejex,
    values= EjeY,
)
data =[GraficaPie]
py.plot(data,filename="sales_1.html")


Rdd = texto.map(lambda linea:linea.split('|'))\
    .filter(lambda linea:(linea[1] != ""))\
    .filter(lambda linea:(linea[1] != "Country" and linea[5] != ""))\
    .filter(lambda linea:(linea[1].upper() == "GUATEMALA".upper()))\
    .map(lambda linea:(linea[5].split("/")[2],int(linea[8])))\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda linea: linea[1],ascending=False)

print("EJERCICIO2")

Ejex = []
EjeY = []
for datos in Rdd.collect():
    Ejex.append("Anio-" + str(datos[0]))
    EjeY.append((datos[1]))
    print("Anio->" + str(datos[0]) + "  Unidades Vendidas->" + str(datos[1]))

GraficaBarras = go.Bar(
    x = Ejex,
    y = EjeY,
    text=EjeY,
    textposition='auto'
)

data = [GraficaBarras]
py.plot(data,filename="sales_2.html")

print("EJERCICIO3")
Rdd_1 = texto.map(lambda linea:linea.split('|'))\
    .filter(lambda linea:(linea[3] != "" and linea[13] != "" and linea[5] != ""))\
    .filter(lambda linea:(linea[3].upper() == "Online".upper()))\
    .filter(lambda linea:(linea[5].split("/")[2] == "2010"))\
    .map(lambda linea:("TOTAL_VENTAS-2010",float(linea[11])))\
    .reduceByKey(lambda x,y:x+y)
Rdd_2 = texto.map(lambda linea:linea.split('|'))\
    .filter(lambda linea:(linea[3] != "" and linea[13] != "" and linea[5] != "" and linea[12] != "" and linea[11] != "" ))\
    .filter(lambda linea:(linea[3].upper() == "Online".upper()))\
    .filter(lambda linea:(linea[5].split("/")[2] == "2010"))\
    .map(lambda linea:("TOTAL_COSTOS-2010",float(linea[12])))\
    .reduceByKey(lambda x,y:x+y)

Rdd_3 = texto.map(lambda linea:linea.split('|'))\
    .filter(lambda linea:(linea[3] != "" and linea[13] != "" and linea[5] != "" and linea[12] != "" and linea[11] != "" ))\
    .filter(lambda linea:(linea[3].upper() == "Online".upper()))\
    .filter(lambda linea:(linea[5].split("/")[2] == "2010"))\
    .map(lambda linea:("TOTAL_GANANCIAS-2010",float(linea[13])))\
    .reduceByKey(lambda x,y:x+y)

print(Rdd_1.collect())
print(Rdd_2.collect())
print(Rdd_3.collect())

Ejex = []
EjeY = []
for datos in Rdd_1.collect():
    Ejex.append(datos[0])
    EjeY.append((datos[1]))

for datos in Rdd_2.collect():
    Ejex.append(datos[0])
    EjeY.append((datos[1]))

for datos in Rdd_3.collect():
    Ejex.append(datos[0])
    EjeY.append((datos[1]))

GraficaBarras = go.Bar(
    x = Ejex,
    y = EjeY,
    text=EjeY,
    textposition='auto'
)

data = [GraficaBarras]
py.plot(data,filename="sales_3.html")