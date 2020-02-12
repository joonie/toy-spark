## Spark mini project

- Jdk 1.8
- Scala.version 2.11.7
- Spark-core 2.3.4

- HDFS
  - hdfs://dev-shop-collection-nn-ncl:8020/user/irteamsu
  - web-ui http://dev-shop-collection-nn-ncl.nfra.io:9870/explorer.html#/

- Auto Build & Spark-submit Script

  -  <details><summary>~/script/build.sh [class] [jar] [intput-file] ...</summary>
     <div markdown="1">
     ./build.sh sparkcollection.acumulator.AccumulatorMain ~/share/accumulator/scala-park-sample-1.0-SNAPSHOT.jar /user/irteamsu/input/statePopulation.csv
     </div></details>

    
- study doc
  - https://github.com/joonie/scalaStudy
