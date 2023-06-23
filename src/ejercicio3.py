# Ejercicio 3:\tAlertas

import sqlite3

import pandas as pd

query = '''
    SELECT k.ip, k.vulnerabilidades_detectadas, a.timestamp, a.prioridad
    FROM alertas a 
    JOIN (
        SELECT * FROM maquinas m JOIN analisis s
        WHERE s.id = m.id
        ) as k 
    WHERE a.destino=k.ip
'''

# Connect to the database
con = sqlite3.connect('data/practica1.db')

# Read the data from the SQL query and store it in the 'table' DataFrame
table = pd.read_sql_query(query, con)

# Filter by alert priority (1, 2, 3)
priority_1 = table[table['prioridad'] == 1]
priority_2 = table[table['prioridad'] == 2]
priority_3 = table[table['prioridad'] == 3]

# Get tables by month
july = pd.read_sql_query(query + " AND STRFTIME('%m',timestamp) in ('07')", con)
august = pd.read_sql_query(query + " AND STRFTIME('%m',timestamp) in ('08')", con)

# Close the connection since we don't need it anymore
con.close()

# Calculate the number of observations for each priority
num_obs_p1 = priority_1.shape[0]
num_obs_p2 = priority_2.shape[0]
num_obs_p3 = priority_3.shape[0]

# Calculate the number of missing values for each priority
num_missing_p1 = priority_1['vulnerabilidades_detectadas'].isnull().sum()
num_missing_p2 = priority_2['vulnerabilidades_detectadas'].isnull().sum()
num_missing_p3 = priority_3['vulnerabilidades_detectadas'].isnull().sum()

# Calculate the mode for each priority
mode_p1 = priority_1['vulnerabilidades_detectadas'].mode().values[0]
mode_p2 = priority_2['vulnerabilidades_detectadas'].mode().values[0]
mode_p3 = priority_3['vulnerabilidades_detectadas'].mode().values[0]

# Calculate the median for each priority
median_p1 = priority_1['vulnerabilidades_detectadas'].median()
median_p2 = priority_2['vulnerabilidades_detectadas'].median()
median_p3 = priority_3['vulnerabilidades_detectadas'].median()

# Calculate the first and third quartiles for each priority
q1_p1 = priority_1['vulnerabilidades_detectadas'].quantile(0.25)
q3_p1 = priority_1['vulnerabilidades_detectadas'].quantile(0.75)

q1_p2 = priority_2['vulnerabilidades_detectadas'].quantile(0.25)
q3_p2 = priority_2['vulnerabilidades_detectadas'].quantile(0.75)

q1_p3 = priority_3['vulnerabilidades_detectadas'].quantile(0.25)
q3_p3 = priority_3['vulnerabilidades_detectadas'].quantile(0.75)

# Calculate the maximum and minimum values for each priority
max_p1 = priority_1['vulnerabilidades_detectadas'].max()
max_p2 = priority_2['vulnerabilidades_detectadas'].max()
max_p3 = priority_3['vulnerabilidades_detectadas'].max()

min_p1 = priority_1['vulnerabilidades_detectadas'].min()
min_p2 = priority_2['vulnerabilidades_detectadas'].min()
min_p3 = priority_3['vulnerabilidades_detectadas'].min()

# Calculate the number of observations for each month
num_obs_july = july.shape[0]
num_obs_august = august.shape[0]

# Calculate the number of missing values for each month
num_missing_july = july['vulnerabilidades_detectadas'].isnull().sum()
num_missing_august = august['vulnerabilidades_detectadas'].isnull().sum()

# Calculate the mode for each month
mode_july = july['vulnerabilidades_detectadas'].mode().values[0]
mode_august = august['vulnerabilidades_detectadas'].mode().values[0]

# Calculate the median for each month
median_july = july['vulnerabilidades_detectadas'].median()
median_august = august['vulnerabilidades_detectadas'].median()

# Calculate the first and third quartiles for each month
q1_july = july['vulnerabilidades_detectadas'].quantile(0.25)
q3_july = july['vulnerabilidades_detectadas'].quantile(0.75)

q1_august = august['vulnerabilidades_detectadas'].quantile(0.25)
q3_august = august['vulnerabilidades_detectadas'].quantile(0.75)

# Calculate the maximum and minimum values for each month
max_july = july['vulnerabilidades_detectadas'].max()
max_august = august['vulnerabilidades_detectadas'].max()

min_july = july['vulnerabilidades_detectadas'].min()
min_august = august['vulnerabilidades_detectadas'].min()

#####################
# PRINT THE RESULTS #
#####################

# POR PRIORIDAD
print("Resultados para la variable de vulnerabilidades detectadas en base a la agrupación por prioridad de alerta")
print("-----------------------------------------------------------------------------------------------------------")
# Número de observaciones
print("===Nº OBSERVACIONES===")
print("prioridad 1:\t", num_obs_p1)
print("prioridad 2:\t", num_obs_p2)
print("prioridad 3:\t", num_obs_p3)
print()
# Número de valores ausentes
print("===AUSENTES===")
print("prioridad 1:\t", num_missing_p1)
print("prioridad 2:\t", num_missing_p2)
print("prioridad 3:\t", num_missing_p3)
print()
# Moda
print("===MODA===")
print("prioridad 1:\t", mode_p1)
print("prioridad 2:\t", mode_p2)
print("prioridad 3:\t", mode_p3)
print()
# Mediana
print("===MEDIANA===")
print("prioridad 1:\t", median_p1)
print("prioridad 2:\t", median_p2)
print("prioridad 3:\t", median_p3)
print()
# Cuartiles Q1 y Q3
print("===CUARTILES===")
print("Cuartil Q1 para la prioridad 1:\t", q1_p1)
print("Cuartil Q3 para la prioridad 1:\t", q3_p1)
print()
print("Cuartil Q1 para la prioridad 2:\t", q1_p2)
print("Cuartil Q3 para la prioridad 2:\t", q3_p2)
print()
print("Cuartil Q1 para la prioridad 3:\t", q1_p3)
print("Cuartil Q3 para la prioridad 3:\t", q3_p3)
print()
# Valores máximo y mínimo
print("===MINIMOS Y MAXIMOS===")
print("Valor mínimo para la prioridad 1:\t", min_p1)
print("Valor mínimo para la prioridad 2:\t", min_p2)
print("Valor mínimo para la prioridad 3:\t", min_p3)
print()
print("Valor máximo para la prioridad 1:\t", max_p1)
print("Valor máximo para la prioridad 2:\t", max_p2)
print("Valor máximo para la prioridad 3:\t", max_p3)
print()
# POR MES
print("\nResultados para la variable de vulnerabilidades detectadas en base a la agrupación por mes:\t")
print("-----------------------------------------------------------------------------------------------------------")
# Número de observaciones
print("===Nº DE OBSERVACIONES===")
print("julio:\t", num_obs_july)
print("agosto:\t", num_obs_august)
print()
# Número de valores ausentes
print("===AUSENTES===")
print("julio:\t", num_missing_july)
print("agosto:\t", num_missing_august)
print()
# Moda
print("===MODA===")
print("julio:\t", mode_july)
print("agosto:\t", mode_august)
print()
# Mediana
print("===MEDIANA===")
print("julio:\t", median_july)
print("agosto:\t", median_august)
print()
# Cuartiles Q1 y Q3
print("===CUARTILES===")
print("Cuartil Q1 para julio:\t", q1_july)
print("Cuartil Q3 para julio:\t", q3_july)
print()
print("Cuartil Q1 para agosto:\t", q1_august)
print("Cuartil Q3 para agosto:\t", q3_august)
print()
# Valores máximo y mínimo
print("===MINIMOS Y MAXIMOS===")
print("Valor mínimo para julio:\t", min_july)
print("Valor máximo para julio:\t", max_july)
print()
print("Valor mínimo para agosto:\t", min_august)
print("Valor máximo para agosto:\t", max_august)
print()
