Ankus
=====
[![Build Status](https://travis-ci.org/cdgore/Ankus.png?branch=master)](https://travis-ci.org/cdgore/Ankus)

Hadoop MapReduce jobs to supplement your Mahout workflow, automating steps for Mahout SSVD and/or PCA pipelines

**an·kus** – _noun_: a tool used by elephant riders (mahouts) to control their elephant

Currently includes Hadoop jobs for:

* Obtaining the column means for a given set of Mahout VectorWritable rows.  This is useful for preprocessing when using Mahout's PCA function.
* Performing vector multiplication (dot products wTx) for a small set of vectors w (stored in Hadoop DistributedCache) and a much larger set of vectors x (stored as VectorWritables on HDFS).  In the ideal use case |x| >> |w|
* Performing matrix multiplication of two matrices stored as VectorWritables.  This is a wrapper around Mahout's matrix multiplcation mappers and reducers, allowing for matrix dimensions to be specified as a location of a json file containing them and also the input and final output directories
* Performing matrix tranposition (wrapper for Mahout's tranposition mappers and reducers), allowing for specification of matrix dimension stored in a json file and also the final input and output directories
* Subtracting a given vector (in DistributedCache) from all vectors given.  This is also useful for Mahout PCA
