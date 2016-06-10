# README #

This README indicates the steps necessary to build and run the tumorsim application, crash recovery, monitoring and analytics, data extracts to Excel, Tableau and Phylogenetic json trees.

## NAMES: tumor_20160602.scala

## AUTHOR: James Napier

## SUMMARY
  
Stickbreaking Epistasis is a fitness landscape model consistent with commonly observed patterns of adaptive evolution. This system addresses the bookkeeping of the genotype during tumor growth from germ cell to final detectable size.

To better understand the effects to fitness by distribution and magnitude of mutations, this system allows the generation of many varieties of tumors by controlling the mutation rates and selection coefficients. Further notes on how to compute the fitness is given at http:www.genetics.org/content/190/2/655.full.pdf

## HIGH SPEED ALGORITHM CONSIDERATIONS
Hierarchical normalized linked list stores common subclonal attributes for all cells within the same clonal mutation; thus, allowing limited memory and reduces replicating fitness calculation. 

## SOFTWARE/HARDWARE ENVIRONMENT

The following open-source platforms and programming languages were used for tumor creation, monitoring and analysis:


### Software Tools

* Apache Hadoop (HortonWorks 2.6.0) - software framework written in Java for distributed storage and processing of very large data sets over commodity hardware clusters
* Apache Hive (1.2.1 spark HiveMetastoreConnection version 1.2.1, interactive hive-cli-0.14) – external data warehousing stacked on Hadoop, provides simulation monitoring, data sumarization, query and analysis
* Apache Scala (2.10.5) - functional programming language that utilizes the JVM (Java Virtual Machine) for platform independency, controls tumor simulation logic
* Apache Spark (1.6.0 with a min of 1.4.0) - distributed computing framework originally developed at UC Berkeley AMPLab, stacked on top of Scala and distributed over Hadoop, tracks tumor array memory across multiple machines
* Bash (Sun AMD64 Linux 2.6.32-504.el6.x86_64) - Bash is a UNIX shell and command language; various scripts used for monitoring, analysis and data export to spreadsheets or other visualizations
* D3.v3.min.js - a javascript graphic extention for displaying a specific tumor phylogenetic tree in a web browser
* Java/JDK (Oracle 1.7) - Spark, Scala and Hive are converted to run in the java virtual machine for platform independetcy
* Microsoft Excel (2013) - for aggregates and table visualizations
* Module (3.2.10) - Bash environment management scripting un/loader
* Tableau (public 9.1 to 9.3) - for tumor bubble plots
* YARN (2.2.4.2-2) - Yet Another Resource Manager, manages Hadoop data and hardware resources 



### Hadoop Cluster Configuration

The computation and data intensive piece includes a 44 node HDP 2.3 cluster on Dell PowerEdge 720xd servers. Each of the 40 data nodes has 128GB ram, 2x Intel E5-2640 6 core processors and 22TB of disk. The cluster backbone network consists of 10Ge HA top of rack switching combined with Intel x520 10Ge NICs in each server. 

This robust combination of server and network architecture provide the foundation for efficiently running the latest New Generation Sequencing (NGS) tools and pipelines in the big data framework. Although the tumor simulator can run parallel jobs utilizing multiple resources, the demands upon the Hadoop NameNode (worker, memory, disk resource management) are quite exhaustive; therefore, it is suggested to run sequential jobs on a single node for as many images as needed to emulate parallelization.


## Special coding considerations

* use Databricks package to read/write tsv instead of parquet
* utilized hive Metastore for external table views
* utilized hive over Tez for rapid analytics of tables in memory
* tumors will grow until extinction, medically detectable or 100 years, whichever comes first
* added parellelization via Futures for multiple tumors but this tends to overload the namenode
* convert years to generationsPerYear = 122 equivalent to 3 days per generation
* annual and final snapshots are stored as tab delimited files in hdfs as well as a final population density table
* utilizing software modules system from HPC to load environments in Hadoop cluster
* hard coded tumor classes for specific tumor number ranges to control number of detectables

### Shipping DOS code to Linux: dos2unix with vi command sequence
```
#!python
<esc>:set fileformat=unix<enter><esc>:wq<enter>

```

## Useful screen commands that allow the simulator to run in the background while logged out
```
#!python

### To start a persistent shell:
screen -S spark
screen -rd spark
screen -X -S spark quit
screen -wipe dead.pid

### If prompt disappears
stty icanon echo > /dev/null 2>&1
```

## Initialize spark environment
```
#!python
module load spark/1.6.0

### this is the contents of the spark environment configuration module file
#%Module1.0#######################################################################
## sge modulefile
##
proc ModulesHelp { } {

        puts stderr "\tAdds spark-1.6.0 to your PATH and MANPATH."
}

module-whatis   "Adds spark-1.6.0 binaries to your path."

set topdir/usr/hdp/2.2.4.2-2/spark-1.6.0
setenv          YARN_CONF_DIR   /etc/hadoop/conf
setenv          SPARK_HOME      $topdir
prepend-path    PATH            $topdir/bin
prepend-path    LD_LIBRARY_PATH $topdir/lib
```


## Start the simulator
```
#!python

### with spark-shell
module load spark/1.6.0; tumorargs="help" spark-shell --master yarn --packages com.databricks:spark-csv_2.10:1.3.0 --driver-memory 8G --executor-memory 16G --executor-cores 1 --num-executors 1 --conf spark.akka.frameSize=16  -i ~/tumor_20160602scala


### with spark-submit
spark-submit --class main.scala.Tumor --master yarn --packages com.databricks:spark-csv_2.10:1.3.0  --deploy-mode client --driver-memory 8G --executor-memory 16G --executor-cores 1 --num-executors 1 --conf spark.akka.frameSize=16 --conf spark.shuffle.spill=false --conf spark.shuffle.compress=true ~/Tumor/target/Tumor-1.0-SNAPSHOT.jar sequential 500000 500009

### help for optional opperands at the end look like the following:
Usage: <add|del|mult|repair> <start#> [<mult_end#>]

```


## Crash recovery:  manual partial tumor removal
```
#!python

hive -e "create database if not exists tumorsim location '/project/tumorsim'"
hive -e 'use tumorsim; show partitions tumors' | grep 284
hive -e "use tumorsim;alter table tumors drop if exists partition (tumorid='AB9_284')"
hadoop dfs -ls /project/tumorsim/tumors/tumorid=AB9_284
hadoop dfs -ls /project/tumorsim/populations/tumorid=AB9_284
hadoop dfs -rmr /project/tumorsim/tumors/tumorid=AB9_284
hadoop dfs -ls /project/tumorsim/tumors | grep tumor | sort | tail
hadoop dfs -ls /project/tumorsim/tumors/*/* | grep tumor | sort | tail
hadoop dfs -ls /project/tumorsim/tumors/* | grep "finalsnapshot=Y" | sort | tail

### to see hdfs partitions
hadoop dfs -ls /project/tumorsim/tumors/*/* | grep "finalsnapshot="  | sort | tail

```

## TUMOR VISUALIZATION

There are two visualizations that were produced to visually analyze the tumor simulation outcomes. The subclone heterogeneity composition visualization is performed in Tableau. The tumor phylogeny visualization is performed using a D3.js tree viewer. 


 
###Export Tumors from Hive to Tab Delimited Files 

Provided below is the Hive commands to create tab-delimited files that can be used to create Excel files used for import of data to Tableau. The commands work on individual tumors. 

```
#!python
### detectable tumors at final snapshot with only the live subclones for a single tumor class
hive -e "set hive.execution.engine=tez;set hive.cli.print.header=true; select * from tumorsim.tumors_detectable where substr(tumorid,1,3) = 'AA9' and cells > 0 and finalsnapshot = 'Y' order by split(tumorid,'_')[1] , finalsnapshot , year, subclone" | sed 's/tumors_detectable.//g' > /home/project/tumorsim/tumors_detectable_final_live_AA9.xls

### detectables exports by tumorclass
hive -e "set hive.execution.engine=tez;set hive.cli.print.header=true; select * from tumorsim.populations_detectable where substr(tumorid,1,3) = 'AA9' order by split(tumorid,'_')[1] , generation" | sed 's/populations_detectable.//g' > /home/project/tumorsim/populations_detectable_AA9.xls


### full exports
hive -e "set hive.cli.print.header=true; select '_c0',* from tumorsim.populations_tabs order by tumorid , generation" | grep -E '^\_c0'| cut -c5- | sed 's/populations.//g' > /home/project/tumorsim/populations.txt
hive -e "set hive.cli.print.header=true; select '_c0',* from tumorsim.tumors_tabs order by tumorid , generation" | grep -E '^\_c0'| cut -c5- | sed 's/tumors.//g' > /home/project/tumorsim/tumors.txt

### copy as comma delimited as well;
cat tumors_detectable_final_live_AA9.xls | tr "\\t" "," >    tumors_detectable_final_live_AA9.csv
```

### Tableau Workbook and Data

The Hive data that is exported to Excel is stored on the [Google Drive](https://drive.google.com/drive/folders/0B6tYGHXcUgsINS1nVzN5VFVhTGc). There is a README.txt file which describes the files and subfolder contents. 

The subfolder [TumorData_final](https://drive.google.com/drive/folders/0B4N7oQdFEBSxNFd3YlRUTy1NYnc) contains the data and workbook used for Diego's final paper. It contains a consolidated Excel file, tumor_final.xlsx, that concatenates all the individual tumors into one that is used by the Tableau workbook, TumorSim-DiegoThesis.twbx. The consolidated Excel file concatenation was done using an add-on Excel plugin called PowerQuery, using the following command:

```
#!python

# PowerQuery Excel command to concatenate all tumor files.
= Table.Combine({Append2,#"Sheet1 (3)",#"Sheet1 (4)",#"Sheet1 (5)",#"Sheet1 (6)",#"Sheet1 (7)",#"Sheet1 (8)",#"Sheet1 (9)",#"Sheet1 (10)"})

```

Additional PowerQuery formulas can be found on the [Microsoft site](https://msdn.microsoft.com/en-us/library/mt253322.aspx). 

The latest Tableau Subclone visualization has been generated and loaded to [Public Tableau](https://public.tableau.com/profile/publish/TumorSim-DiegoThesis/Sheet1#!/publish-confirm). 


### Phylogenetic tree exports

The phylogenetic trees are used to show a given tumor's family tree of daughter mutations. The Hive extraction routines have been specifically designed to trace through the daughter mutations to the final surviving mutations. If one of those final mutations have descended from a parent that subsequently dies, the connection to the grandmother subclones will not be lost. 

The visualization was created using a D3.js tree structure. 

Further details are found in the downloads document: "Phylogenetic Tree_DesignDocument.docx"

Sample trees can also be found in the download area named like:  tree_AA9_300413.html
```
#!python
### create a json object for each detectable tumor to hdfs
source tumors_to_json.sh

### export a single tumor json object for d3.js to browse the phylogenetic tree
tumorid=BD9_20308 hive -e "set hive.execution.engine=tez; select 'var flare = ' , json from tumorsim.detectable_json_children where tumorid='$tumorid'" | tr -d '\"' | sed 's/\t//g' > $tumorid.js
```

## Analytics written in Hive/SQL to create tumor class aggregates into a spreadsheet called tumor_evolution.xls
```
#!python
tumor_analysis.hive
```

## Monitoring actively long running simulations written in Hive and Bash scripting
```
#!python
tumor_monitor.hive
```