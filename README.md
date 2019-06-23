# Processing and analysis of TEMAR data

METAR(METeorological Aerodrome Report) is a worldwide standard format of weather reporting. In this project, 
we use observations of 'ASOS' to perform data processing, data storage, data visualization for some analytical purposes.

## Getting Started

Files to be executed: <br/>
> plot_indicator_over_time.py<br/>
> map_indicator.py<br/>
> cluster_indicators.py<br/>


### Prerequisites

Cassandra, PySpark, matplotlib, numpy...

## Instructions for running the files.

Question1: Plot indicators over time
```
Python plot_indicator_over_time.py --station_id='LFST' --indicateur='tmpf'
```
Question2: Plot indicators of a specific time in a map
```
Python map_indicator.py 
```
Question3: Plot clustering
```
Python cluster_indicators.py 
```

## Authors

* **Anthony GALTIER** - *Initial work* - [Github profile](https://github.com/anthonygal)
* **Zhiqi KANG** - *Initial work* - [Github profile](https://github.com/kangzhiq)
