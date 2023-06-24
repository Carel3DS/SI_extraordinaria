import sqlite3
import pandas as pd
import matplotlib.pyplot as plt


# Ejecuta la consulta SQL y cargar los resultados en un DataFrame
query = '''
    SELECT a.destino,
       a.origen,
       a.timestamp,
       a.prioridad,
       a.clasificacion,
       k.servicios_inseguros,
       k.servicios,
       k.id,
       k.vulnerabilidades_detectadas,
       k.puertos_abiertos
    FROM alertas a
    JOIN (SELECT * FROM analisis a
            JOIN (SELECT m.id, m.ip, COUNT(*) AS puertos_abiertos
                FROM puertos p
                INNER JOIN maquinas m on p.id = m.id GROUP BY m.id
            ) AS m
            ON a.id = m.id
        ) AS k
    ON (a.origen = k.ip or a.destino = k.ip);
'''
# Conecta a la BD
con = sqlite3.connect('data/practica1.db')
df = pd.read_sql_query(query, con)

# Cerramos la conexión a la base de datos
con.close()

# Ajustamos tamaño vertical de los plots
plt.figure(figsize=(10,7))

# Tarea 1: Mostrar las 10 IP de origen más problemáticas en un gráfico de barras
top_10_ips = df[df['prioridad'] == 1]['origen'].value_counts().head(10)
plt.bar(top_10_ips.index, top_10_ips.values)
plt.xlabel('IP de Origen')
plt.xticks(rotation=30) # Rota las etiquetas de las ordenadas para mejor visibilidad
plt.ylabel('Número de Alertas')
plt.title('Top 10 IP de Origen más Problemáticas')
plt.show()

# Tarea 2: Número de alertas a lo largo del tiempo en un histograma
df['timestamp'] = pd.to_datetime(df['timestamp'])
plt.hist(df['timestamp'], bins=30, edgecolor='black')
plt.xlabel('Tiempo')
plt.xticks(rotation=15)
plt.ylabel('Número de Alertas')
plt.title('Número de Alertas a lo largo del Tiempo')
plt.tight_layout()

plt.show()
# Tarea 3: Porcentaje del total de alertas por categoría en un gráfico circular
plt.figure(figsize=(10,10))

conteo_alertas = df['clasificacion'].value_counts()

etiquetas = ['{0} - {1:1.3f} %'.format(i,j) for i,j in zip(df['clasificacion'].unique(), (conteo_alertas/conteo_alertas.sum())*100)]

plt.title('Porcentaje del Total de Alertas por Categoría')
plt.pie(conteo_alertas, startangle=90, radius=1)

# Añadir etiquetas fuera del gráfico
plt.legend(etiquetas, bbox_to_anchor=(0.9, 0))
plt.tight_layout()
plt.show()


#Añadir ejes

# Tarea 4: Dispositivos más vulnerables basados en la suma de servicios vulnerables y vulnerabilidades detectadas
df['punt_vuln'] = df['servicios_inseguros'] + df['vulnerabilidades_detectadas']

#Agrupamos por id y los ordenamos de mayor a menor
top_vuln = df.groupby('id')[['punt_vuln']].max().sort_values(by='punt_vuln', ascending=False)
plt.bar(top_vuln.index, top_vuln['punt_vuln'])
plt.xlabel('Dispositivo')
plt.ylabel('Puntuación de Vulnerabilidad')
plt.title('Top Dispositivos más Vulnerables')

plt.xticks(rotation=30)
plt.figure(figsize=(10,7))
plt.show()

# Tarea 5: Media de puertos abiertos en comparación con servicios inseguros y el total de servicios detectados
media_puertos_abiertos = df['puertos_abiertos'].mean()
media_servicios_inseguros = df['servicios_inseguros'].mean()
media_total_servicios = df['servicios'].mean()
plt.bar(['Puertos Abiertos', 'Servicios Inseguros', 'Total de Servicios'],
        [media_puertos_abiertos, media_servicios_inseguros, media_total_servicios])
plt.ylabel('Media')
plt.title('Media de Puertos Abiertos, Servicios Inseguros y Total de Servicios')
plt.show()



