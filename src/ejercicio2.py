# EJERCICIO 2: Simple ETL
import sqlite3

import pandas as pd
import json

# Note: Data is already extracted

######################
# TRANSFORM AND LOAD #
######################

# Connect to the database
con = sqlite3.connect('data/practica1.db')
cur = con.cursor()

# Open the devices info as JSON and transform into SQL
with open("data/devices.json") as f:
    data = json.load(f)
    df = pd.DataFrame(data)

    # Extract analisis and puertos, and create an extended dataframe
    analisis = []
    ports = []

    for i in range(df.__len__()):
        line = df['analisis'][i]
        line['id'] = df['id'][i]
        port = {'id': line['id'], 'puertos': line.pop('puertos_abiertos')}
        if port['puertos'] == "None":
            port['puertos'] = ["None"]

        ports.append(pd.DataFrame(port))
        analisis.append(pd.Series(line))

    analisis = pd.concat(analisis, axis=1).transpose()
    ports = pd.concat(ports, ignore_index=True)

    # Extract responsable and create usuarios dataframe
    usuarios = []
    nombres = []
    for i in range(df.__len__()):
        line = df['responsable'][i]
        nombres.append(line['nombre'])  # Save names to craft maquinas dataframe
        if line['nombre'] not in [s['nombre'] for s in usuarios]:
            usuarios.append(pd.Series(line))  # Save unique usuarios

    nombres = pd.Series(nombres)
    usuarios = pd.concat(usuarios, axis=1).transpose()

    # Drop 'analisis' column, replace 'responsable' with 'nombres' array and merge with 'analisis' dataframe
    maquinas = df.drop(columns='analisis')
    maquinas['responsable'] = nombres

    # Save ports and final df to database
    ports.to_sql('puertos', con, if_exists='replace')  # 'Replace' avoids failure when filling the table
    usuarios.to_sql('usuarios', con, if_exists='replace')
    maquinas.to_sql('maquinas', con, if_exists='replace')
    analisis.to_sql('analisis', con, if_exists='replace')

# Extract transform CSV into alertas table
with open("data/alerts.csv") as f:
    alertas = pd.read_csv(f)
    alertas.to_sql('alertas', con, if_exists='replace')

# TEST the database
# cur.execute("SELECT * FROM usuarios JOIN maquinas ON maquinas.responsable=usuarios.nombre ")

# Commit changes and close
con.commit()
con.close()

############
# ANALYSIS #
############

# Use all the dataframes we created to analyze data

# Prepare Ports dataframe for analysis
portGroup = ports[ports['puertos'] != "None"].groupby('id').count()

# COUNTS
devices = maquinas.__len__()  # Number of devices (1)
alerts = alertas.__len__()  # Number of alerts (2)
numNones = maquinas \
               .merge(usuarios, right_on="nombre", left_on="responsable") \
               .merge(analisis, on="id").isin(["None"]).sum().sum() \
           + ports.isin(["None"]).sum().sum()

# MEDIANS
medianPorts = portGroup['puertos'].median()
medianInsec = analisis['servicios_inseguros'].median()
medianVulns = analisis['vulnerabilidades_detectadas'].median()

# MODES
modePorts = portGroup['puertos'].mode()
modeInsec = analisis['servicios_inseguros'].mode()
modeVulns = analisis['vulnerabilidades_detectadas'].mode()
# Check if there's really a mode or not:
if len(list(modePorts)) == len(list(portGroup)):
    modePorts = None
else:
    modePorts = modePorts.to_list()

if len(list(modeInsec)) == len(list(analisis)):
    modeInsec = None
else:
    modeInsec = modeInsec.to_list()

if len(list(modeVulns)) == len(list(analisis)):
    modeVulns = None
else:
    modeVulns= modeVulns.to_list()

# MINIMUMS
minVulns = analisis['vulnerabilidades_detectadas'].min()
minPorts = portGroup['puertos'].min()

# MAXIMUMS
maxVulns = analisis['vulnerabilidades_detectadas'].max()
maxPorts = portGroup['puertos'].max()

print(
    "=== STATISTICS ===",
    f"devices:\t\t{devices}",
    f"alerts:\t\t\t{alerts}",
    f"numNones:\t\t{numNones}\n",
    "=== INSECURE SERVICES ===",
    f"medianInsec:\t{medianInsec}",
    f"modeInsec:\t\t{modeInsec}\n",
    "=== VULNERABILITIES ===",
    f"medianVulns:\t{medianVulns}",
    f"modeVulns:\t\t{modeVulns}",
    f"minVulns:\t\t{minVulns}",
    f"maxVulns:\t\t{maxVulns}\n",
    "=== OPEN PORTS ===",
    f"medianPorts:\t{medianPorts}",
    f"modePorts:\t\t{modePorts}",
    f"minPorts:\t\t{minPorts}",
    f"maxPorts:\t\t{maxPorts}",
    sep="\n"
)

