# GeoTrellis on EMR tutorial

This project serves as an example of how to work with the National Elevation Dataset on GeoTrellis and Spark on EMR.
We will be

## Introduction

### What is the data?

### What do we want to do with it?

Paint awesome base maps!

### ETL

### What is EMR?

## Getting started

### Setting up the environment

#### Building the project

#### Accessing EMR

#### Setting the EMR environment

##### How many instances should I use?

#### Starting the cluster

#### Accessing the cluster UIs

#### Exploring our data with Zeppelin

## Painting our first basemap

### Basics of ETL

### Creating our source dataset

###

## Dynamic vs Static

##




## Inspecting the data with Zeppelin

https://gist.github.com/echeipesh/766fcf60d0bbb389adb4

To add Zeppelin to the cluster, you'll have to add `Name=Zeppelin-Sandbox` to the
list of applications when launching the cluster. You can do this with the
scripts by uncommenting the `ZEPPELIN="Name=Zeppelin-Sandbox"` line in the `environment.sh` file.

Start Zeppelin by enabling the web connection via SSH tunnelling and foxyproxy.

Edit the spark configuration with the proper configuration, and then restart the spark context.

Execute the code to pull down the GeoTrellis dependency:

```
%dep
z.reset()
z.load("com.azavea.geotrellis:geotrellis-spark_2.10:0.10.0-RC3")
```

Now we can start using GeoTrellis to write scala code and execute it on our cluster.
