package scala.tumor;
// ****************************************************************************************;
// *** Names: tumor_20151013.scala
//
// *** Author: James Napier
//
// *** Summary:  Stickbreaking Epistais is a fitness landscape model consistent with commonly observed patterns of adaptive evolution.
//  This sytem addresses the bookkeeping of the genotype during tumor growth from germ cell to final detectable size.
//  To better understand the effects to fitness by distribution and magnitude of mutations, this sytem allows the generation of many
//  varieties of tumors by controlling the mutation rates and selection coefficients.
//  Further notes on how to compute the Fitness is given at http://www.genetics.org/content/190/2/655.full.pdf + html
//
//  *** todo:
//    convert braches array to broadcast variables to reduce akka memory issues
//    test spark 1.5.1
//    add to bitbucket
//    look into figtree software for visualizing branches over time snapshots
//    ADD ASSUMPTIONS AND GOTCHAS FOR THE WHOLE PROGRAM AND EACH METHOD
//    intall intellij and look for errors
//
//  *** updates as of 20151013
//    added more comments
//    added cutoff at 50 years
//    added mutation_selection value and resistant indicator
//
// ****************************************************************************************;
// dos2unix with vi command sequence::: <esc>:set fileformat=unix<enter><esc>:wq<enter>
// ****************************************************************************************;
//
//  *** usefull screen commands
//    screen -S spark
//    screen -rd spark
//    screen -X -S spark quit
//    screen -wipe dead.pid
//
//  *** if prompt dissapears
//    stty icanon echo > /dev/null 2>&1
//
//  *** initialize spark environment
//    source /etc/spark/conf/spark-1.3.0-env.sh
//    source /etc/spark/conf/spark-1.4.0-env.sh
//    source /etc/spark/conf/spark-1.4.1-env.sh
//    source /etc/spark/conf/spark-1.5.1-env.sh
//
// *** to create one tumor from command line
//   export tumorargs="help"; spark-shell --master yarn --driver-memory 16G --executor-memory 16G --executor-cores 15 --num-executors 30 --conf spark.akka.frameSize=32 -i tumor_20151013.scala
//   export tumorargs="add 284"; spark-shell --master yarn --driver-memory 16G --executor-memory 16G --executor-cores 1 --num-executors 2 --conf spark.akka.frameSize=32 -i tumor_20151013.scala &
//   export tumorargs="add 285"; spark-shell --master yarn --driver-memory 16G --executor-memory 16G --executor-cores 1 --num-executors 2 --conf spark.akka.frameSize=32 -i tumor_20151013.scala &
//   export tumorargs="add 284"; spark-shell --master yarn --driver-memory 16G --executor-memory 16G --executor-cores 1 --num-executors 2  -i tumor_20151008.scala &
//   export tumorargs="del AA9_283"; spark-shell --master yarn --driver-memory 16G --executor-memory 16G --executor-cores 1 --num-executors 2 --conf spark.akka.frameSize=32 -i tumor_20151013.scala
//
// *** to create multiple tumors from command line
//   export tumorargs="mult 284 286"; spark-shell --master yarn --driver-memory 16G --executor-memory 16G --executor-cores 1 --num-executors 2 --conf spark.akka.frameSize=32 -i tumor_20151013.scala
//
// *** to parallelize tumors
//   for tumornum in `seq 284 286`; do (export tumorargs="add $tumornum"; spark-shell --master yarn --driver-memory 16G --executor-memory 16G --executor-cores 1 --num-executors 2 --conf spark.akka.frameSize=32 -i tumor_20151013.scala) & if (( $tumornum % 10 == 0 )); then wait; fi done;
//
// *** to manually remove a parital tumor due to a crash
//   hive -e "create database if not exists tumorsim location '/project/tumorsim'"
//   hive -e 'use tumorsim; show partitions tumors' | grep 284
//   hive -e "use tumorsim;alter table tumors drop if exists partition (tumorid='AB9_284')"
//   hadoop dfs -ls /project/tumorsim/tumors/tumorid=AB9_284
//   hadoop dfs -ls /project/tumorsim/populations/tumorid=AB9_284
//   hadoop dfs -rmr /project/tumorsim/tumors/tumorid=AB9_284
//   hadoop dfs -ls /project/tumorsim/tumors | grep tumor | sort | tail
//    tumor 283 was last completed
//
// *** to see hdfs partitions
//     hadoop dfs -ls /project/tumorsim/tumors/*/* | grep "finalsnapshot="  | sort | tail
//
// *** turn off spark shuffle spill to disk - ask Bharath
//
// *** memory failure before adding akka framesize option to command line
//   WARN TaskSetManager: Stage 2 contains a task of very large size (10352 KB). The maximum recommended task size is 100 KB.
//   INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 4, istb1-l2-b14-09.hadoop.priv, PROCESS_LOCAL, 10600738 bytes)
//   INFO YarnScheduler: Cancelling stage 2
//   INFO YarnScheduler: Stage 2 was cancelled
//   INFO DAGScheduler: ResultStage 2 (parquet at <console>:94) failed in 0.508 s
//   INFO DAGScheduler: Job 2 failed: parquet at <console>:94, took 0.628880 s
//   ERROR InsertIntoHadoopFsRelation: Aborting job.
//   Caused by: org.apache.spark.SparkException: Job aborted due to stage failure: Serialized task 4:0 was 10652805 bytes, which exceeds max allowed: spark.akka.frameSize (10485760 bytes) - reserved (204800 bytes).
//   Consider increasing spark.akka.frameSize or using broadcast variables for large values.
//
//
// ****************************************************************************************;
// ****************************************************************************************;
// ****************************************************************************************;

import scala.collection.mutable.ArrayBuffer                             // needed to create the array of branches
import org.apache.spark.sql.hive._                                      // needed to save permanent parquet tables in hive
import org.apache.commons.math3.distribution.ExponentialDistribution    // for exponential random number distribution
import sys.process._                                                    // see http://alvinalexander.com/scala/scala-execute-exec-external-system-commands-in-scala to access hive from shell to get around access issues
//import org.apache.spark.SparkContext                                  // only needed if compiling with sbt or maven

//create table/database statements fail due to incorrect authorizor package linked between spark and hive
//  FAILED: RuntimeException org.apache.hadoop.hive.ql.metadata.HiveException: java.lang.ClassNotFoundException: org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory
//  http://grepcode.com/file/repository.cloudera.com$content$repositories$releases@org.apache.hive$hive-exec@1.1.0-cdh5.4.2@org$apache$hadoop$hive$ql$security$authorization$plugin$sqlstd$SQLStdConfOnlyAuthorizerFactory.java
//  the package that is supposed to clean this up is noted below, but still requires hiveconf2.xml modifications, so this appears to be a todo item for the apache team
//  package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;
//    import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
//    import org.apache.hadoop.hive.conf.HiveConf;
//    import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
//    import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
//    import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
//    import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerImpl;
//    import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
//    import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
//    import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;

// ****************************************************************************************
// build Tumor Simulator as an object with methods for single or multiple tumor generation and recovery
// ****************************************************************************************

object Tumor {

  // the following two lines can not be done within spark-shell, but can with sbt or maven, because the spark-shell implicitly does this upon startup
   val conf = new SparkConf().setAppName("tumorsim")
   val sc = new SparkContext(conf)

  val hc = new org.apache.spark.sql.hive.HiveContext(sc)  // initialize the hive context before creating a dataframe

  // ****************************************************************************************
  // define classes
  // ****************************************************************************************

  // define the branch class here so that growTumor can return the correct type
  class branch
  (
    val subClone            : Int
    , val parent              : Int
    , val mutation            : Double
    , val beta                : Double
    , val fitness             : Double
    , var cells               : Int
    , var mutations           : Int
    , var dob                 : Int
    , var dod                 : Int
    , var maxpop              : Int
    , var deaths              : Int
    , val mutation_selection  : Double
    , val resistant           : String
    )

  // Define the tumors parquet schema using a case class
  case class tumor_schema
  (
    subClone            : Int
    , parent              : Int
    , mutation            : Double
    , beta                : Double
    , fitness             : Double
    , cells               : Int
    , mutations           : Int
    , dob                 : Int
    , dod                 : Int
    , maxpop              : Int
    , deaths              : Int
    , mutation_selection  : Double
    , resistant           : String
    )

  // define the genPop class to track population at each generation for any given tumor
  class genPop ( val generation : Int , var population : Int )

  // Define the populations parquet schema using a case class
  case class genPop_schema ( generation : Int , population : Int )

  // ****************************************************************************************
  // create a method to save a tumor at any time to hive parquet partitioned by tumorid,finalsnapshot,year
  // ADD MORE NOTES HERE, AND I/O
  // ****************************************************************************************

  def saveTumor(
                 // ****************************************************************************************
                 // default configs
                 // ****************************************************************************************
                 tumorid : String = "1"          // ADD COMMENT HERE FOR EACH DEFAULT VALUE
                 , finalsnapshot : String = "Y"
                 , year : Double = 0.0
                 , tumor : ArrayBuffer[branch]
                 ): Unit =
  {
    //println("tumorid="+tumorid+" , finalsnapshot="+finalsnapshot+" , year="+year)

    // convert the tumor to an RDD then to DF so that we can seve to HDFS and Hive metabase
    val tumorRDD = tumor.map(b => tumor_schema(b.subClone,b.parent,b.mutation,b.beta,b.fitness,b.cells,b.mutations,b.dob,b.dod,b.maxpop,b.deaths,b.mutation_selection,b.resistant))
    import hc.implicits._           // load hive context implicits before converting RDD to DataFrame
  val tumorDF = tumorRDD.toDF()   // data frame required to write to hive tables
    //println("/project/tumorsim/tumors/tumorid="+tumorid+"/finalsnapshot="+finalsnapshot+"/year="+year)

    // send a copy of the tumor to hdfs partition as parquet
    // this method works in v1.4.0+
    tumorDF.write.parquet("/project/tumorsim/tumors/tumorid="+tumorid+"/finalsnapshot="+finalsnapshot+"/year="+year)
    // this method works in 1.3.0-
    //tumorDF.saveAsParquetFile("/project/tumorsim/tumors/tumorid="+tumorid+"/finalsnapshot="+finalsnapshot+"/year="+year)

    // update the hive metabase with by adding a new partition for the tumor just saved to hdfs
    val addPartition = "alter table tumors add if not exists partition (tumorid='"+tumorid+"',finalsnapshot='"+finalsnapshot+"',year="+year+") location '/project/tumorsim/tumors/tumorid="+tumorid+"/finalsnapshot="+finalsnapshot+"/year="+year+"'"
    //println(addPartition)
    hc.sql(addPartition)
  }

  // tumorDF.printSchema
  //
  //   root
  //   |-- subClone: integer (nullable = false)
  //   |-- parent: integer (nullable = false)
  //   |-- mutation: double (nullable = false)
  //   |-- beta: double (nullable = false)
  //   |-- fitness: double (nullable = false)
  //   |-- cells: integer (nullable = false)
  //   |-- mutations: integer (nullable = false)
  //   |-- dob: integer (nullable = false)
  //   |-- dod: integer (nullable = false)
  //   |-- maxpop: integer (nullable = false)
  //   |-- deaths: integer (nullable = false)
  //   |-- mutation_selection: double (nullable = false)
  //   |-- resistant: string (nullable = false)
  //   |-- tumorid: integer (nullable = false)
  //   |-- finalsnapshot: integer (nullable = false)
  //   |-- year: integer (nullable = false)

  // ****************************************************************************************
  // create a method to save the population density by generation for a tumor saved at final snapshot to hive parquet partitioned by tumorid,finalsnapshot,year
  // ****************************************************************************************

  def savePopulation(
                      // ****************************************************************************************
                      // default configs
                      // ****************************************************************************************
                      tumorid : String = "1"
                      , populationHistory : ArrayBuffer[genPop]
                      ): Unit =
  {

    // convert the population history to an RDD then to DF so that we can seve to HDFS and Hive metabase
    val populationHistoryRDD = populationHistory.map(p => genPop_schema(p.generation,p.population))
    import hc.implicits._
    val populationHistoryDF = populationHistoryRDD.toDF()

    // send a copy of the population history to hdfs partition as parquet
    //println("/project/tumorsim/populations/tumorid="+tumorid)
    // this method works in v1.4.0+
    populationHistoryDF.write.parquet("/project/tumorsim/populations/tumorid="+tumorid)
    // this method works in 1.3.0-
    //populationHistoryDF.saveAsParquetFile("/project/tumorsim/populations/tumorid="+tumorid)

    // update the hive metabase with by adding a new partition for the population history just saved to hdfs
    val addPopPartition = "alter table populations add if not exists partition (tumorid='"+tumorid+"') location '/project/tumorsim/populations/tumorid="+tumorid+"'"
    //  println(addPopPartition)
    hc.sql(addPopPartition)
  }

  // ****************************************************************************************
  // grow a tumor and write annual and final snapshot to hive partitioned parquet tables and possibly to screen
  // ****************************************************************************************

  def growTumor (
                  // ****************************************************************************************
                  // default configs
                  // ****************************************************************************************
                  tumorid : String = "1"
                  , lambda : Double = 0.005                                 // for exponential random distribution, where average centered around this value
                  // test 2 classes: 0.1 for 50% of the tumors, 0.005 for the other 50%
                  , driver_mutation_rate : Double = 3.4 * math.pow(10,-3)   // how often a cell that does not die, splits with a mutation
                  // test 3 classes: 1*10^-4 and 1*10^-5 and 1*10^-7
                  // 2 lambdas and 3 mutation rates for 6 categories
                  , maxPopulation : Int  = math.pow(10,6).toInt             // number of cells the tumor must grow to detectable completion
                  , w_wt : Double = 1.0                                     // fitness centering
                  , d : Double = 1.0                                        // fitness slope
                  , printToScreen : Boolean = false                         // useful to send small tumors to screen capture
                  , includeDeadBranches : Boolean = true                    // whether to include dead branches when printing to screen, usefull to turn off if scraping to excel
                  , maxTumorGrowthYears : Int = 50                          // stop growing the tumor if it takes more than 50 years

                  ) : ArrayBuffer[branch] =                                   // returns the full tumor in case we want to manipulate, browse, clone, etc...
  {

    println("Working on tumorid = "+tumorid)

    // ****************************************************************************************
    // random number generators
    // ****************************************************************************************

    def ifprintln( s : String ) = if ( printToScreen ) println( s ) // used to print to screen for small tumors, when screen scraping

    def ranuni = scala.util.Random.nextDouble                       // used for death and mutation selection coefficients

    def exprnd = new ExponentialDistribution( lambda ).sample       // used for mutation coefficient

    // ****************************************************************************************;
    // start with the first germcell, also track then number of mutations at each branch and population at each generation
    // ****************************************************************************************;

    var finalshapshot = "N"                 // this is an incomplete tumor, when turned on ="Y" then a final snapshot of the tumor will be displayed/saved
  var subClone = 0                        // no prior subclones exist, since this is the germ cell
  var generation = 1                      // first generation created this germ cell
  var parent = -1                         // first cell has no parent cell reference: branches._-1 does not exist
  var mutation  = exprnd                  // the mutation pressure from an exponential random distribution for stickbreaking accounting
  var beta = ( 1 - mutation )             // partial fitness equation, showing complete lineage of all mutations to date
  var fitness = w_wt + d * ( 1 - beta )   // fitness of this subclonal cellular branch, used later to determine cellular death or mutation upon splitting
  var cells = 1                           // current number of cells in this branch
  var mutations = 1                       // cumulative mutations going back thru full lineage
  var dob = generation                    // date of birth or the generation in which this branch was created
  var dod = 0                             // date of death, in which the last cell died
  var maxpop = 1                          // maximum population density in this branch to date
  var deaths = 0                          // cumulative cells killed off in this branch to date
  var mutation_selection = 1.0            // mutation selection coefficient, when larger than fitness, the split cell will gain a mutation causing a new branch, germcell has a definite mutation
  var resistant = if (mutation_selection < math.pow(10,-7)) 1 else 0     // very tiny mutations are considered drug resistant

    // add a new branch
    var branches = ArrayBuffer( new branch( subClone,parent,mutation,beta,fitness,cells,mutations,dob,dod,maxpop,deaths,mutation_selection,resistant))
    // jnj convert braches array to broadcast variables

    // track the populaiton
    var population = 1
    var populationHistory = ArrayBuffer( new genPop(generation,population) )   // track population at each generation = 3 days

    // display germcell
    ifprintln("subClone="+subClone+" ,parent="+parent+" ,mutation="+mutation+" ,beta="+beta+", fitness="+fitness+", cells="+cells+", mutations="+mutations+", dob="+dob )
    ifprintln("branches(0).cells="+branches(0).cells)

    // ****************************************************************************************
    // after the germcel is created, we proceed with tumor evolution - death/split/mutate
    // ****************************************************************************************

    do
    {
      generation = generation + 1   // begin the next generation cellular deaths, splits or mutations

      for ( thisBranch <- 0 to branches.size -1 )   // all known branches at the beginning of this generation
      {
        // ****************************************************************************************
        // ifprintln("working on thisBranch = " + thisBranch + " with cells = " + branches(thisBranch).cells)
        // ****************************************************************************************

        var maxCells = branches(thisBranch).cells
        var mutation_selection = ranuni                             // mutation selection coefficient, when larger than fitness, the split cell will gain a mutation causing a new branch
      var resistant = (mutation_selection < math.pow(10,-7))      // very tiny mutations are considered drug resistant

        for ( cell <- 1 to maxCells )                                           // test each individual cell in this branch to determine if it will die/split/mutate
        {
          if ( population > 10 && ranuni > branches(thisBranch).fitness / 2)    // dont let the population drop below 10 cells, but possibly kill off cells that are not fit enough
          {
            // ****************************************************************************************
            // cell death due to low fitness
            // ****************************************************************************************
            branches(thisBranch).cells  -= 1     // death
            branches(thisBranch).deaths += 1     // death
            // ifprintln("branch("+thisBranch+").cell("+cell+") death:  branches("+thisBranch+").cells = " + branches(thisBranch).cells)
            if ( branches(thisBranch).cells == 0 ) { branches(thisBranch).dod = generation }    // date subClone population fully died off
            population -= 1
          }
          else if ( mutation_selection < driver_mutation_rate )     // a rare mutation has occured when the cell split
          {
            // ****************************************************************************************
            // add a new branch/subclone with one new mutated daughter cell
            // ****************************************************************************************

            subClone = branches.size                            // the next branch number to be created
            parent = thisBranch                                 // track lineage where this new branch was derived from
            mutation  = exprnd                                  // the mutation pressure from an exponential random distribution for stickbreaking accounting
            beta = ( 1 - mutation ) * branches(parent).beta     // partial fitness equation, showing complete lineage of all mutations to date

            // example of the compounding of inherited mutations stored as a partial equation to be passed on to the next generation
            // mutation 1 :   beta = (1 - mutation1)                                            first mutation germcell has no prior mutated parent
            // mutation 2 :   beta = (1 - mutation1) * (1 - mutation2)                                      = (1 - mutation2) * parent_beta
            // mutation 3 :   beta = (1 - mutation1) * (1 - mutation2) * (1 - mutation3)                    = (1 - mutation3) * parent_beta
            // mutation 4 :   beta = (1 - mutation1) * (1 - mutation2) * (1 - mutation3)  * (1 - mutation4) = (1 - mutation4) * parent_beta

            fitness = w_wt + d * ( 1 - beta )   //  = w_wt + d * ( 1 - (1-u1) * (1-u2) * (1-un) )

            cells = 1
            mutations = branches(parent).mutations + 1
            dob = generation
            dod = 0
            maxpop = 1
            deaths = 0

            branches += new branch( subClone,parent,mutation,beta,fitness,cells,mutations,dob,dod,maxpop,deaths,mutation_selection,resistant)
            // ifprintln("***** mutation ***** parent.mutation="+ branches(parent).mutation +" this.mutation="+mutation)
            // ifprintln("subClone="+subClone+" ,parent="+parent+" ,mutation="+mutation+" ,beta="+beta+", fitness="+fitness+", cells="+cells+", mutations="+mutations+", dob="+dob )
            population += 1
          }
          else
          {
            // ****************************************************************************************
            // split without mutation just increases the population in this branch
            // ****************************************************************************************
            branches(thisBranch).cells += 1
            branches(thisBranch).maxpop = math.max(branches(thisBranch).maxpop , branches(thisBranch).cells )
            // ifprintln("branch("+thisBranch+").cell("+cell+") division:  branches("+thisBranch+").cells = " + branches(thisBranch).cells)
            population += 1
          }
          //ifprintln("tumorid="+tumorid+"  generation="+generation+"  population="+population+"  branches="+branches.size)
        }
      }
      // ****************************************************************************************
      // track population history at the end of each generation and possibly show annual snapshot
      // ****************************************************************************************

      populationHistory += new genPop(generation,population)

      var year = math.round(generation*3.0/366*10)/10.0     // generation / 122

      var finalsnapshot = if ( population >= maxPopulation || year >= maxTumorGrowthYears ) {"Y"} else {"N"}  // stopping point of tumor growth, when it reaches a certain size or has grown for too many years

      if ( (generation % 122 == 0 && year > 0.0 ) || finalsnapshot == "Y" ) // annual snapshot = 122 populations * 3 days each = 366 days, or the tumor has finished growing
      {
        saveTumor(tumorid,finalsnapshot,year,branches)

        if ( printToScreen )
        {
          if ( year == 1.0 ) { ifprintln("year,branch,parent,mutation,beta,fitness,cells,mutations,dob,dod,population,maxpop,deaths") }
          for ( branch <- 0 to branches.size -1)
          {
            if ( branches(branch).cells > 0 || includeDeadBranches == true )  // graphing software matpotlib/plotli may need to include even the dead branches
            {
              ifprintln(year+","+branches(branch).subClone+","+branches(branch).parent+","+branches(branch).mutation+","+branches(branch).beta+","+branches(branch).fitness+","+branches(branch).cells+","+branches(branch).mutations+","+branches(branch).dob+","+branches(branch).dod+","+population+","+branches(branch).maxpop+","+branches(branch).deaths)
            }
          }
        }
      }
    } while ( population < maxPopulation && year < maxTumorGrowthYears )

    // save the chearleader, save the world -- its what any hero would do
    savePopulation(tumorid,populationHistory)

    // log final stats
    var finalYear = math.round(generation*3.0/366*10)/10.0;
    val quote = '"'
    println("tumorid= "+tumorid+"  final_generation= " + generation + "  years= " + finalYear + "  population= " + population)
    var saveTumorText = "saveTumor(tumorid="+quote+tumorid+quote+", finalsnapshot="+quote+"Y"+quote+",year="+finalYear+",tumor=tumor_"+tumorid+")"
    println("to save the final snapshot manually:")
    println(saveTumorText)

    // return the tumor incase we ever want to do further analysis or clone it
    branches : ArrayBuffer[branch]
  }

  // ****************************************************************************************;
  // heres an example of how to create a single tumor and then also manually save a clone
  // ****************************************************************************************;

  // val tumor_10to5_1 =  growTumor(tumorid = "10to5_1" , lambda = 0.1 , driver_mutation_rate = math.pow(10,-5) , maxPopulation = math.pow(10,5).toInt)
  // tumorid= 10to5_1  final_generation= 69  years= 0.6  population= 113120
  // saveTumor(tumorid="10to5_1_2", finalsnapshot="Y",year=0.3,tumor=tumor_10to5_1)

  // ****************************************************************************************;
  // build a mod ringed tumorid based on its ordinal number
  // ****************************************************************************************;

  def buildTumorid( tumorNum: Int , numberOfTumorClasses: Int , tumorSize : Int = 9 ) : String =
  {
    var tumorClass    = tumorNum % numberOfTumorClasses
    var lambdaClass   = "A"
    var mutationClass = "A"

    if (tumorClass == 1) { lambdaClass="A" ; mutationClass = "A" }
    if (tumorClass == 2) { lambdaClass="A" ; mutationClass = "B" }
    if (tumorClass == 3) { lambdaClass="A" ; mutationClass = "C" }

    if (tumorClass == 4) { lambdaClass="B" ; mutationClass = "A" }
    if (tumorClass == 5) { lambdaClass="B" ; mutationClass = "B" }
    if (tumorClass == 0) { lambdaClass="B" ; mutationClass = "C" }

    var tumorid = lambdaClass + mutationClass + tumorSize + "_" + tumorNum

    tumorid
  }

  // ****************************************************************************************;
  // grow one specialized class tumor mod 6;  tumorSize is medically identifyable at 10^9 cells but adjustable for testing
  // ****************************************************************************************;

  def growOneSpecializedTumor( tumorNum : Int , numberOfTumorClasses : Int = 6 , tumorSize: Int = 9 ) : Unit =
  {
    var tumorClass = tumorNum % numberOfTumorClasses
    var lambda = 0.1
    var driver_mutation_rate = math.pow(10,-4)
    var maxPopulation = math.pow(10,tumorSize).toInt

    if (tumorClass == 1) { lambda = 0.1   driver_mutation_rate = math.pow(10,-4) }
    if (tumorClass == 2) { lambda = 0.1   driver_mutation_rate = math.pow(10,-5) }
    if (tumorClass == 3) { lambda = 0.1   driver_mutation_rate = math.pow(10,-7) }

    if (tumorClass == 4) { lambda = 0.005 driver_mutation_rate = math.pow(10,-4) }
    if (tumorClass == 5) { lambda = 0.005 driver_mutation_rate = math.pow(10,-5) }
    if (tumorClass == 0) { lambda = 0.005 driver_mutation_rate = math.pow(10,-7) }

    var tumorid = buildTumorid( tumorNum , numberOfTumorClasses , tumorSize )

    println("Working on:  tumorid="+tumorid+", maxPopulation="+maxPopulation)

    val tumor =  growTumor( tumorid , lambda , driver_mutation_rate , maxPopulation )
  }

  // ****************************************************************************************;
  // save multiple tumors in a permanent parquet table on hdfs
  // ****************************************************************************************;

  def multTumors( start : Int =  1 , end : Int = 1 , core_slices : Int = 10 ) : Unit =
  {
    // sequential version
    //   for (t <- start to end ) growOneSpecializedTumor( t )

    // parallel version
    sc.parallelize(start to end, core_slices ).map(t => growOneSpecializedTumor( t ))
  }

  // multTumors(start = 1 , end = 1000 )  // this would create 1000 specialized tumors in the mod6 ring

  // ****************************************************************************************
  // remove an partial tumor due to mid gen crash
  // ****************************************************************************************

  def removeTumor( tumorNum : Int , numberOfTumorClasses : Int = 6 , tumorSize: Int = 9 ) : Unit =
  {
    var tumorid = buildTumorid( tumorNum , numberOfTumorClasses , tumorSize )
    hc.sql("alter table tumors      drop if exists partition (tumorid='"+tumorid+"')")
    hc.sql("alter table populations drop if exists partition (tumorid='"+tumorid+"')")
  }

  // ****************************************************************************************
  // initialize hive metastore
  // ****************************************************************************************
  def  initializeHive()
  {
    //import hc.implicits._
    // due to authorization failure, tried to override HiveServer2-site.xml, but security will not allow it
    //hc.sql("set hive.security.authorization.enabled=true")
    //hc.sql("set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider")
    hc.sql("set hive.exec.failure.hooks=")
    hc.sql("set hive.exec.pre.hooks=")
    hc.sql("set hive.exec.post.hooks=")
    hc.sql("set hive.cli.print.header=true")
    //hc.sql("create database if not exists tumorsim location '/project/tumorsim'") // authorization failure
    hc.sql("use tumorsim")
    // authorization failure
    //hc.sql("create table if not exists tumorSim.tumors ( subClone Int , parent Int , mutation Double , beta Double , fitness Double , cells Int , mutations Int , dob Int , dod Int , maxpop Int , deaths Int ) partitioned by ( tumorid String , finalsnapshot String , year double ) stored as parquet");
    //hc.sql("create table if not exists tumorSim.populations ( generation Int , population Int ) partitioned by ( tumorid String ) stored as parquet");

    //**************************************************************************************************
    // can access a subset via hive with limit statement, but faster would be take(8).collect()
    //**************************************************************************************************
    // hc.sql("FROM tumors SELECT * limit 8").collect().foreach(println)
    // or
    // hc.sql("FROM tumors SELECT * ").take(8).collect().foreach(println)
    // combine both limit and take?
  }

  // ****************************************************************************************
  // main function needs to initialize hive metastore and grow one or more tumors
  // ****************************************************************************************

  //  def main ( args: Array[String] = System.getenv("tumorargs").split("\\s+") )
  def main ( argsString: String)
  {
    val spark_home = System.getenv("SPARK_HOME")
    val min_spark_ver = "/usr/hdp/2.2.4.2-2/spark-1.4.0"

    // make sure we are using the minimum spark version or newer
    if ( spark_home < min_spark_ver )
    {
      System.err.println( "Invalid spark environment loaded: " + spark_home )
      System.err.println( "Minimum spark version is: " + min_spark_ver )
      System.exit(1)
    }
    else if (argsString.length > 0) // something passed to the main method, convert to array of args
    {
      var args = argsString.split("\\s+")
    }
    else if (argsString.length == 0) // nothing passed to the main method, get args from environment
    {
      var args = System.getenv("tumorargs").split("\\s+")
      if (args.length == 0)
      {
        System.err.println("Usage: Tumor <del|add|mult|repair> <startTumor#> [<endTumor#>]")
        System.exit(1)
      }
    }

    // ****************************************************************************************
    // something maybe valid passed as runtime arguments, lets go find out what
    // ****************************************************************************************

    args foreach println
    //println("args="+args.toString)

    initializeHive()  // only needed if we passed some parms

    // ****************************************************************************************
    // one argument means to create one tumor, two arguments is for a range of tumors
    // ****************************************************************************************

    if ( args(0) == "add"    && args.length == 2 ) { growOneSpecializedTumor( args(1).toInt ) }
    else if ( args(0) == "del"    && args.length == 2 ) { removeTumor( args(1).toInt ) }
    else if ( args(0) == "mult"   && args.length == 3 ) { multTumors( start = args(1).toInt , end = args(2).toInt ) }
    else if ( args(0) == "repair" && args.length == 2 ) { removeTumor( args(1).toInt ) ; growOneSpecializedTumor( args(1).toInt ) }
    // anthing else looks like help
    else                                                { println("Usage: <add|del|mult|repair> <start#> [<mult_end#>]") }
  }
}


// *** manual method to add/del a single tumor
//Tumor.main ("add 284")
//Tumor.main ("del AB9_284")

//Tumor.main()      // run arguments passed via environment variable $tumorargs
//System.exit(0)  // shut down spark shell when done with this automated set



