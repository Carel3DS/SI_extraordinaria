# Ejercicio 3: Alertas

import sqlite3

import pandas as pd

# Connect to the database
con = sqlite3.connect('data/practica1.db')

# Create the dataframes by month and group by priority.
# Note: Months are July and August
countAlertasByMonth = pd.read_sql_query("SELECT COUNT(*) as recuento, STRFTIME('%m',timestamp) as mes "
                                        "FROM alertas "
                                        "WHERE mes in ('07', '08') "
                                        "GROUP BY mes", con)

countAlertasByPrioridad = pd.read_sql_query("SELECT COUNT(*) as recuento, "
                                            '''
                                            CASE
                                                WHEN prioridad = 1 THEN 'Alertas graves' 
                                                WHEN prioridad = 2 THEN 'Alertas medias' 
                                                WHEN prioridad = 3 THEN 'Alertas bajas' 
                                                ELSE 'Otras alertas' 
                                             END AS prioridad
                                            '''
                                            "FROM alertas "
                                            "GROUP BY prioridad"
                                            , con)

alertas = pd.read_sql_query('''
                             SELECT CASE
                                WHEN  STRFTIME('%m',timestamp) = '07' THEN 7
                                WHEN  STRFTIME('%m',timestamp) = '08' THEN 8
                             END AS mes, prioridad
                             FROM alertas 
                             WHERE mes in (7,8)
                            ''', con)
missing = pd.read_sql_query("SELECT * FROM alertas WHERE STRFTIME('%m',timestamp) in ('07','08')", con).isnull().sum()\
                                                                                                                .sum()

# Close the database since we finished using it
con.close()

############
# ANALYSIS #
############

# TOTALS
totalByMes = alertas.groupby('mes').count()
totalByPrioridad = alertas.groupby('prioridad').count()

# MEDIANS
medianPrioridad = alertas['prioridad'].median()
medianMes = alertas['mes'].median()

# MODES
modePrioridad = countAlertasByPrioridad.mode()
modeMonth = countAlertasByMonth.mode()

# QUARTILES
qPrioridad = alertas['prioridad'].quantile(q=[0.25, 0.75])
qMes = alertas['mes'].quantile(q=[0.25, 0.75])


# RESULTS
print(
    f'''
    === RESULTADOS ===
    Campos Nulos: {missing}
    Mediana mes: 
    '''
)


