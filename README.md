# movielens

_Authors: Caleb Carlson, Glade Snyder_

Repository dedicated to teamwork efforts on the [CS555: Distributed Systems](https://www.cs.colostate.edu/~cs555/) 
[Assignment 3](docs/CS555-Fall2021-HW3.pdf) *(click link for description)*. 
Performs data processing and analytics on the MovieLens datasets, created by GroupLens hosted on Kaggle.
These datasets total over 1 GB and contain movie ratings, titles, tags, and user reviews.

Project written in Scala, built using Scala Build Tool (SBT), and uses a Spark cluster across 10 machines managed by the Yarn Resource Manager.
Data hosted on a Hadoop Distributed Filesystem (HDFS) cluster, and read into Spark where it is processed in-memory.

## Analytics

### Assignment Questions

1. How many movies were released for every year within the dataset? The title column of movies.csv includes the year 
each movie was published. Some movies might not have the year, in such cases you can ignore those movies.
2. What is the average number of genres for movies within this dataset?
3. Rank the genres in the order of their ratings? Again, a movie may span multiple genres; such a 
movie should be counted in all the genres.
4. What are the top-3 combinations of genres that have the highest ratings?
5. How many movies have been tagged as “comedy”? Ignore the “case” information (i.e. both “Comedy” and “comedy” should be considered).
6. What are the different genres within this dataset? How many movies were released within different genres? A movie may span multiple genres; in such cases, that movie should be counted in all the genres?
7. Given a user-defined time period, what are the most popular movies in that time period based on ratings assigned? 

### Assignment Answers

See the [analytics](docs/CS555_HW3.pdf) for answers to the above questions, and short descriptions of how these analytics were performed.

## Usage

Using the predefined scripts, which you may change to your liking:

- Remove the output answer directories in HDFS from any previous executions by running `./clean_answers.sh`.
- Update the project for any new git commits, and rebuild with `./refresh.sh`
- Submit the built jar from SBT using the `./submit.sh <input_dir> <output_dir>` script along with the desired input and output directories:
  - Example: `submit.sh hdfs://<HDFS_NAMENODE>:<HDFS_PORT>/cs555/data hdfs://<HDFS_NAMENODE>:<HDFS_PORT>/cs555/output`

Not using the predefined scripts:

- Build the project using Scala Build Tool (SBT): `sbt clean compile`. This compiles the project and packages it as a jar under
`target/scala-<version>/movielens_<scala_version>-<project_version>.jar`
- Submit the built jar to a Spark manager, specifying the directory containing the MovieLens dataset `.csv`s, and the directory
of your choosing for outputting results:

```bash
HADOOP_CONF_DIR="<hadoop_conf_home>" "${SPARK_HOME}"/bin/spark-submit \
    --class org.movielens.Application \
    --master (local|yarn|standalone) \
    --deploy-mode cluster \
    --driver-memory <driver_memory> \
    --executor-memory <executor_memory> \
    --executor-cores <executor_cores> \
    <path_to_jar> "<input_dir>" "<output_dir>"
```