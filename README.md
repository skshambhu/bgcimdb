# IMDB Top 20 Ranking
BGC IMDB Movies Ratings

##How to build and deploy
1. Clone this repo: `https://github.com/skshambhu/bgcimdb.git`
2. To run the unit test `sbt test`
3. To create Jar file run `sbt 'set test in assembly := {}' clean assembly`. Jar file will be created at `target/scala-2.11/ImdbAgg-assembly-1.0.jar`
4. Download data from `https://datasets.imdbws.com/`
5. Update `scala.properties` to all  configurations details

`master=local[*]
 appName=BCG IMDB Ratings
 title.ratings=/Users/sanjay/projects/bgc/data/source/title.ratings.tsv.gz
 title.basics=/Users/sanjay/projects/bgc/data/source/title.basics.tsv.gz
 title.akas=/Users/sanjay/projects/bgc/data/source/title.akas.tsv.gz
 title.principals=/Users/sanjay/projects/bgc/data/source/title.principals.tsv.gz
 name.basics=/Users/sanjay/projects/bgc/data/source/name.basics.tsv.gz
 top20Movies.tsv=/Users/sanjay/projects/bgc/data/sink/top20Movies.tsv
 principals.tsv=/Users/sanjay/projects/bgc/data/sink/principals.tsv
 allTitles.tsv=/Users/sanjay/projects/bgc/data/sink/allTitles.tsv
 header=true
 delimiter=\t`
 
 6. Deploy the jar file and provide the location of `scala.properties` file as an argument
 7. Output will be written to the location set above `top20Movies.tsv`, `principals.tsv` and `allTitles.tsv` 

##Project Structure:
`src/main/`
  `scala/com/bgcpartners/imdb/`

    `ImdbAgg.scala`
    
    `Rankings.scala`
    
  `resources/scala/scala.properties`

#Future improvements

1. Add logging capability
2. Add exception handling
