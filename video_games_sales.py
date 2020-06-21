import plotly.offline as py
import plotly.graph_objects as go
from pyspark import SparkContext
sc = SparkContext("local","Practica1")
path1 = "C:\\Users\\Bayyron\\Desktop\\Junio2020\\Seminario2\\Laboratorio\\Archivos\\Libro1.csv"
texto = sc.textFile(path1)
Rdd = texto.map(lambda linea:linea.split("|"))\
    .filter(lambda linea:(linea[4] != "Genre"))\
    .filter(lambda linea:(linea[4] != ""))\
    .filter(lambda linea:(linea[4].upper() == "ACTION" \
                          or linea[4].upper() == "SPORTS"\
                          or linea[4].upper() == "Fighting".upper()\
                          or linea[4].upper() == "Shooter".upper()\
                          or linea[4].upper() == "Racing".upper()\
                          or linea[4].upper() == "Adventure".upper()\
                          or linea[4].upper() == "Strategy".upper()))\
    .map(lambda linea:(linea[4],float(linea[10])))\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda linea: linea[1],ascending=False)
Ejex = []
EjeY = []
for datos in Rdd.collect():
    Ejex.append(datos[0])
    EjeY.append(round(datos[1],2))
    print("Categoria->" + datos[0] + "  Total->" + str(datos[1]))

GraficaBarras = go.Bar(
    x = Ejex,
    y = EjeY,
    text=EjeY,
    textposition='auto'
)

data = [GraficaBarras]
py.plot(data,filename="Video_Games_Sales_1.html")
print("EJERCICIO 2")
##PROCESO PARA EL EJERCICIO 2: Total de Generos publicados Por nintendo
RddNintendo = texto.map(lambda linea:linea.split('|'))\
    .filter(lambda linea:(linea[5].upper() == "Nintendo".upper()))\
    .map(lambda linea:(linea[4]))\
    .map(lambda variable: (variable,1))\
    .reduceByKey(lambda x,y: x+y)
Juegos = []
TotalJuegos = []
for datos in RddNintendo.collect():
    Juegos.append(datos[0])
    TotalJuegos.append(datos[1])
    print("Categoria->" + datos[0] + "  Total de Juegos->" + str(datos[1]))

GraficaPie = go.Pie(
    labels= Juegos,
    values= TotalJuegos
)
data =[GraficaPie]
py.plot(data,filename="Video_Games_Sales_2.html")

print("EJERCICIO 3")
Rdd3 = texto.map(lambda linea:linea.split('|'))\
    .filter(lambda linea:(linea[2].upper() != "".upper()))\
    .map(lambda linea:(linea[2]))\
    .map(lambda variable: (variable,1))\
    .reduceByKey(lambda x,y: x+y)\
    .sortBy(lambda linea: linea[1],ascending=False)
TOP5PLATAFORMAS = Rdd3.take(5)
PLATAFORMAS = []
TOTALJUEGOSPLATAFORMA = []
for datos in TOP5PLATAFORMAS:
    PLATAFORMAS.append(datos[0])
    TOTALJUEGOSPLATAFORMA.append(datos[1])
    print("Categoria->" + datos[0] + "  Total de Juegos->" + str(datos[1]))

GraficaBarras = go.Bar(
    x = PLATAFORMAS,
    y = TOTALJUEGOSPLATAFORMA,
    text=TOTALJUEGOSPLATAFORMA,
    textposition='auto'
)

data = [GraficaBarras]
py.plot(data,filename="Video_Games_Sales_3.html")
