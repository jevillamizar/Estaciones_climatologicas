from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, when, col
import json
import subprocess
import webbrowser

spark = SparkSession.builder.appName("estaciones").getOrCreate()

# Procesamiento de los datos
def procesar_consulta(texto):
    # Creación de lista de nombre de columnas del DF
    columnas_df = ["USAF", "WBAN", "STATION_NAME", "Country", "ST", "CALL", "Latitud", "Longitud", "ELEV", "BEGIN", "END"]

    # Leer archivo csv con SparkSession
    archivo = spark.read.csv('/home/teban94/Enfasis3/trabajo-estaciones/isd-history.csv')
    archivo = archivo.toDF(*columnas_df)
    archivo = archivo.select(col('Country'), col('Latitud'), col('Longitud'))

    # Aplicar las transformaciones necesarias

    archivo = archivo.select(
       
        when(archivo.Country.isNull(),'0').otherwise(archivo.Country).alias('Country'),

        # Remplaza elementos nulos y con algun formato erroneo en la columna 'Latitud'
        regexp_replace(when(archivo.Latitud.isNull() | 
                            (archivo.Latitud == '+00.000') | 
                            (archivo.Latitud == '+000.000'),'0.000'
                            ).otherwise(archivo.Latitud), r'^\+', ''
                        ).alias('Latitud'),

        # Remplaza elementos nulos y con algun formato erroneo en la columna 'Longitud'
        regexp_replace(when(archivo.Longitud.isNull() | 
                            (archivo.Longitud == '+00.000') | 
                            (archivo.Longitud == '+000.000'), '00.000'
                            ).otherwise(archivo.Longitud), r'^\+', ''
                        ).alias('Longitud')
    )

    # Eliminar los ceros de la izq
    archivo = archivo.withColumn("Longitud", regexp_replace("Longitud", "^(-)?0+(?!\\.)(?=\\d)", "$1"))
    archivo = archivo.withColumn("Latitud", regexp_replace("Latitud", "^(-)?0+(?!\\.)(?=\\d)", "$1"))

    query = archivo.filter(archivo['Country'] == texto)

    # comvertir datos a JSON
    json_result = []
    for row in query.rdd.collect():
        json_data = {
            "country": row.Country,
            "latitude": row.Latitud,
            "longitud": row.Longitud
        }
        json_result.append(json_data)    
   
    with open('/home/teban94/Enfasis3/trabajo-estaciones/result.json', 'w') as f:
        f.write(json.dumps(json_result))
    query.show()

# Llamar a la función con el texto deseado
texto_ingresado = "CO"  # Reemplaza con el texto deseado
procesar_consulta(texto_ingresado)

# Ejecutando servidor web
def ejecutar_servidor_web():
    servidor_command = "python3 -m http.server 5500"
    subprocess.call(servidor_command, shell=True)

# Abrir el archivo index.html en el navegador
    webbrowser.open("http://localhost:5500/index.html")

