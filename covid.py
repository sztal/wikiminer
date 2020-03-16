import json
from pathlib import Path
from wikiminer.scripts.covid import get_covid_data

datapath = (Path(__file__).parent / 'data' / 'covid').absolute()
datapath.mkdir(parents=True, exist_ok=True)

data = get_covid_data()

filename = 'wp-covid-19__'+data['datetime'].date().strftime("%Y-%m-%d")+'.json'
filepath = datapath / filename

data['datetime'] = data['datetime'].strftime("%Y-%m-%d %H:%M:%S")

with open(filepath, 'w') as file:
    file.write(json.dumps(data)+"\n")
