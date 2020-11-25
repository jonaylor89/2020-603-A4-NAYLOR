
# CMSC 603 Assignment 3 Hadoop

John Naylor

### abstract
 
Going with the theme of understanding the relationship between algorithms and their parallel counterparts, a parallel version of KNN based on a paper by Maillo, Triguero, and Herrera (2015) which in itself was based on the principle of MapReduce was implemented on Hadoop, a framework for implementing MapReduce jobs on commercially available hardware, and compared against parallel implementations from previous assignments (i.e. Sync, MPI, and CUDA). For small datasets, the data clearly showed that a more traditional method on more advanced hardware like GPUs with CUDA provided better results. But, factoring in the scalability and velocity of programming, the MapReduce implementation run on Hadoop is a great asset to have. 
