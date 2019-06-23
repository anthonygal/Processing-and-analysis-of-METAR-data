# Processing and analysis of METAR data

METAR(METeorological Aerodrome Report) is a worldwide standard format of weather reporting. In this project, 
we use observations of 'ASOS' to perform data processing, data storage, data visualization for some analytical purposes.

## Getting Started
Files to be executed: <br/>
> plot_indicator_over_time.py<br/>
  Plots curves of a given indicator over time with a particular emphasis on seasonal differences.
  
> map_indicator.py<br/>
  Plots a geographical representation of a given indicator at given point in time.
  
> cluster_indicators.py<br/>
  Performs K-Means clustering of the data based on the values of a list of indicators over a given time interval and produces a geogrphical map based visualisation of the results.


### Prerequisites

Cassandra, PySpark, matplotlib, numpy...

ASOS.txt data file downloaded from: https://mesonet.agron.iastate.edu/request/download.phtml, where missing values are recorded as 'M'.


## Instructions for running the files.

Question1: Plot indicators over time
```
Python plot_indicator_over_time.py --station_id='LFST' --indicateur='tmpf'
```

Question2: Plot indicators of a specific time in a map <br/>
Using iPython:
```
>> from map_indicator import getIndicatorMap
>> getIndicatorMap(2001, 7, 23, 16, 'tmpf')
# The resulting PNG figure will be saved to the current working directory.
```



Question3: Plot clustering <br/>
Using iPython:
```
>> from cluster_indicators import kmeans
>> from cluster_indicators import plot_kmeans_map

>> kmeans(7, ['tmpf', 'alti', 'feel'], 2003, 8, 3, 17, 2003, 11, 28, 17) 
# returns the clusters central values and count
# warning: start year must be the same as end year for now

>> plot_kmeans_map(7, ['tmpf', 'alti', 'feel'], 2003, 8, 3, 17, 2003, 11, 28, 17) #warning: start year must be the same as end year for now
# the resulting PNG figure will be saved to the current working directory
# warning: start year must be the same as end year for now

```

## Authors

* **Anthony GALTIER** - *Initial work* - [Github profile](https://github.com/anthonygal)
* **Zhiqi KANG** - *Initial work* - [Github profile](https://github.com/kangzhiq)
