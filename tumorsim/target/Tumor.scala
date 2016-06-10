package main.scala
//package com.databricks:spark-csv_2.10:1.3.0  // instead include on the command line like this: --packages com.databricks:spark-csv_2.10:1.3.0

import org.apache.commons.math3.distribution.ExponentialDistribution
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

// to reduce logging notes in spark shell
//Logger.getLogger("org").setLevel(Level.OFF)
//Logger.getLogger("akka").setLevel(Level.OFF)

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.SQLContext

// ****************************************************************************************
// build Tumor Simulator as an object with methods for single or multiple tumor generation and recovery
// ****************************************************************************************

object Tumor {

  // the following two lines can not be done within spark-shell, but can with sbt or maven, because the spark-shell implicitly does this upon startup
    val conf = new SparkConf().setAppName("tumorsim")
    val sc = new SparkContext(conf)

    val hc = new org.apache.spark.sql.hive.HiveContext(sc)  // initialize the hive context before creating a dataframe           // load hive context implicits before converting RDD to DataFrame           // load hive context implicits before converting RDD to DataFrame
    val sqlContext = new SQLContext(sc)

  // ****************************************************************************************
  // define the branch class here so that growTumor can return the correct type, case class required for parquet schema
  // ****************************************************************************************
  case class branch (
                      val subClone              : Int
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
                      , val resistant           : Int
                      )

  // ****************************************************************************************
  // define the genPop class to track population at each generation for any given tumor, case class required for parquet schema
  // ****************************************************************************************

  case class genPop ( val generation : Int , var population : Int , var mass10to7th : Int , var mass10to8th : Int )

  // ****************************************************************************************
  // random number generators
  // ****************************************************************************************

  def ranuni = scala.util.Random nextDouble()                       // used for death and mutation selection coefficients

  def exprnd ( lambda : Double ) = new ExponentialDistribution( lambda ).sample       // used for mutation coefficient

  // ****************************************************************************************
  // merge multiple reducer outputs outputs into one
  // ****************************************************************************************

  def mergeMove(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.fullyDelete(hdfs , new Path(dstPath))
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
    FileUtil.fullyDelete(hdfs , new Path(srcPath))
  }

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
    val tumorRDD = tumor.map(b => branch(b.subClone,b.parent,b.mutation,b.beta,b.fitness,b.cells,b.mutations,b.dob,b.dod,b.maxpop,b.deaths,b.mutation_selection,b.resistant))
    import hc.implicits._
    val tumorDF = tumorRDD.toDF()   // data frame required to write to hive tables

    //l destPath      = "/project/tumorsim/tumors/tumorid="+tumorid+"/finalsnapshot="+finalsnapshot+"/year="+year
    val destPath      = "/project/tumorsim/tumors.xls"
    val destFile      = tumorid + "_" + finalsnapshot + "_" + year + "_tumor.xls"
    val tmpPath       = "/tmp"
    val tmpPathFile   = tmpPath  + "/" + destFile
    val destPathFile  = destPath + "/" + destFile

    println("saving "+destFile)

    // send a copy of the tumor to hdfs
    //    tumorDF.saveAsParquetFile(destPath)   // this method works in 1.3.0-
    //    tumorDF.write.parquet(destPath)       // this method works in v1.4.0+

    // turned parquet off in v20151230, to replace with tab delimited output
    tumorDF.write.format("com.databricks.spark.csv").option("header","false").option("delimiter","\t").save(tmpPathFile)

    // that just wrote one file per reducer, next we need to combine to one tab delimited file
    mergeMove(tmpPathFile, destPathFile)

    // update the hive metabase with by adding a new partition for the tumor just saved to hdfs
    val addPartition = "alter table tumors add if not exists partition (tumorid='"+tumorid+"',finalsnapshot='"+finalsnapshot+"',year="+year+
      ") location '/project/tumorsim/tumors/tumorid="+tumorid+"/finalsnapshot="+finalsnapshot+"/year="+year+"'"

    // temporarily turned metadata updates off
    //  hc.sql(addPartition)
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
    val populationHistoryRDD = populationHistory.map(p => genPop(p.generation,p.population,p.mass10to7th,p.mass10to8th))
    import hc.implicits._
    val populationHistoryDF = populationHistoryRDD.toDF()

    //l destPath      = "/project/tumorsim/populations/tumorid="+tumorid
    val destPath      = "/project/tumorsim/populations.xls"
    val destFile      = tumorid + "_population.xls"
    val tmpPath       = "/tmp"
    val tmpPathFile   = tmpPath  + "/" + destFile
    val destPathFile  = destPath + "/" + destFile

    // send a copy of the population history to hdfs

    // populationHistoryDF.saveAsParquetFile(destPath)   // this method works in 1.3.0-
    // populationHistoryDF.write.parquet(destPath)      // this method works in v1.4.0+

    // turned parquet off in v20151230, to replace with tab delimited output
    populationHistoryDF.write.format("com.databricks.spark.csv").option("header","false").option("delimiter","\t").save(tmpPathFile)

    // that just wrote one file per reducer, next we need to combine to one tab delimited file
    mergeMove(tmpPathFile , destPathFile)

    // update the hive metabase with by adding a new partition for the population history just saved to hdfs
    val addPopPartition = "alter table populations add if not exists partition (tumorid='"+tumorid+"') location '/project/tumorsim/populations/tumorid="+tumorid+"'"
    //  println(addPopPartition)

    //  temporarily turned metadata updates off
    // hc.sql(addPopPartition)

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
                  , maxPopulation : Int  = math.pow(10,6).toInt             // number of cells the tumor must grow to detectable completion
                  , branches : ArrayBuffer[branch] = ArrayBuffer[branch]()  // track tumor lineage
                  , populationHistory : ArrayBuffer[genPop] = ArrayBuffer[genPop]()            // track population at each generation
                  , newPopulation : Int = 1                                 // start with one germ cell
                  , w_wt : Double = 1.0                                     // fitness centering
                  , d : Double = 1.0                                        // fitness slope
                  , printToScreen : Boolean = false                         // useful to send small tumors to screen capture
                  , includeDeadBranches : Boolean = true                    // whether to include dead branches when printing to screen, usefull to turn off if scraping to excel
                  , maxTumorGrowthYears : Int = 100                         // stop growing the tumor if it takes more than 100 years
                  , generationsPerYear : Int = 122                          // equivalent to 3 days per generation, used to time when to capture annual snapshot
                  , minPopulation : Int = 1                                 // cellular death allowed after population exceeds this, used to be 10

                  ) : ArrayBuffer[branch] =                                 // returns the full tumor in case we want to manipulate, browse, clone, etc...
  {

    def ifprintln( s : String ) = if ( printToScreen ) println( s ) // used to print to screen for small tumors, when screen scraping

    println("Working on tumorid = "+tumorid)

    // ****************************************************************************************;
    // initialize germcell or continue on after chemo
    // ****************************************************************************************;


    // ****************************************************************************************;
    // start with the first germcell, also track then number of mutations at each branch and population at each generation
    // ****************************************************************************************;

    var subClone = 0                        // no prior subclones exist, since this is the germ cell
    var generation = 1                      // first generation created this germ cell
    var parent = -1                         // first cell has no parent cell reference: branches._-1 does not exist
    var mutation  = exprnd(lambda)          // the mutation pressure from an exponential random distribution for stickbreaking accounting
    var beta = 1 - mutation                 // partial fitness equation, showing complete lineage of all mutations to date
    var fitness = w_wt + d * ( 1 - beta )   // fitness of this subclonal cellular branch, used later to determine cellular death or mutation upon splitting
    var cells = 1                           // current number of cells in this branch
    var mutations = 1                       // cumulative mutations going back thru full lineage
    var dob = generation                    // date of birth or the generation in which this branch was created
    var dod = 0                             // date of death, in which the last cell died
    var maxpop = 1                          // maximum population density in this branch to date
    var deaths = 0                          // cumulative cells killed off in this branch to date
    var mutation_selection = 1d             // mutation selection coefficient, when larger than fitness, split cell will gain mutation on new branch, germcell has definite mutation
    var resistant = if (mutation_selection < math.pow(10,-7)) 1 else 0     // very tiny mutations are considered drug resistant

    var mass10to7th : Int = 0               // track in population history indicating found branch with 10^7 cells at each generation
    var mass10to8th : Int = 0               // track in population history indicating found branch with 10^8 cells at each generation
    var population : Int = newPopulation

    var postChemo : Boolean = false

    if ( populationHistory.size == 0 ) {

      // add a new branch
      branches += branch( subClone,parent,mutation,beta,fitness,cells,mutations,dob,dod,maxpop,deaths,mutation_selection,resistant)

      // track the population
      populationHistory += genPop(generation,population,mass10to7th,mass10to8th)

      // display germcell
      ifprintln("subClone="+subClone+" ,parent="+parent+" ,mutation="+mutation+" ,beta="+beta+", fitness="+fitness+", cells="+cells+", mutations="+mutations+", dob="+dob )
      ifprintln("branches(0).cells="+branches.head.cells)

    } else {

      // ****************************************************************************************
      // post chemo initialization
      // ****************************************************************************************

      postChemo = true

    }


    // ******************************************************************************************************************************************************
    // after the germcel is created or a preexisting one is loaded, we proceed with tumor evolution - death/split/mutate
    // ******************************************************************************************************************************************************

    generation = populationHistory(populationHistory.size-1).generation

    var year : Double = math.round(generation/generationsPerYear*10)/10.0     // equivalent to 3 days per generation by default

    do
    {
      generation = generation + 1   // begin the next generation cellular deaths, splits or mutations
      mass10to7th = 0           // track in population history indicating found branch with 10^7 cells at each generation
      mass10to8th = 0           // track in population history indicating found branch with 10^8 cells at each generation

      for ( thisBranch <- branches.indices )   // all known branches at the beginning of this generation
      {
        // ****************************************************************************************
        // ifprintln("working on thisBranch = " + thisBranch + " with cells = " + branches(thisBranch).cells)
        // ****************************************************************************************

        for ( cell <- 1 to branches(thisBranch).cells )                                           // test each individual cell in this branch to determine if it will die/split/mutate
        {
          mutation_selection = ranuni // mutation selection coefficient, when larger than fitness, the split cell will gain a mutation causing a new branch

          if ( population >= minPopulation && ranuni > branches(thisBranch).fitness / 2)    // possibly kill off cells that are not fit enough
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
            mutation  = exprnd(lambda)                          // the mutation pressure from an exponential random distribution for stickbreaking accounting
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
            resistant = if (mutation_selection < math.pow(10,-7)) 1 else 0     // very tiny mutations are considered drug resistant

            branches += branch( subClone,parent,mutation,beta,fitness,cells,mutations,dob,dod,maxpop,deaths,mutation_selection,resistant)
            // ifprintln("***** mutation ***** parent.mutation="+ branches(parent).mutation +" this.mutation="+mutation)
            // ifprintln("subClone="+subClone+" ,parent="+parent+" ,mutation="+mutation+" ,beta="+beta+", fitness="+fitness+", cells="+cells+", mutations="+mutations+", dob="+dob )

            // update population stats
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

            // update population stats
            population += 1
          }
        }

        // finished testing each cell in a given branch
        //ifprintln("tumorid="+tumorid+"  generation="+generation+"  population="+population+"  branches="+branches.size)
        if (branches(thisBranch).cells >= math.pow(10,7).toInt) mass10to7th += 1    // how many branches with 10^7 cells at each generation
        if (branches(thisBranch).cells >= math.pow(10,8).toInt) mass10to8th += 1    // how many branches with 10^8 cells at each generation
      }

      // ****************************************************************************************
      // track population history at the end of each generation and possibly show annual snapshot
      // ****************************************************************************************

      populationHistory += genPop(generation,population,mass10to7th,mass10to8th)

      year = math.round(generation/generationsPerYear*10)/10.0     // equivalent to 3 days per generation by default

      // stopping point of tumor growth, when it reaches a certain size or has grown for too many years
      var finalsnapshot : String = if ( population < minPopulation || population >= maxPopulation || year >= maxTumorGrowthYears ) {"Y"} else {"N"}

      if ( (generation % generationsPerYear == 0 && year > 0.0 ) || finalsnapshot == "Y" ) // annual snapshot or the tumor has finished growing or extinct
      {
        if (postChemo == true) finalsnapshot = finalsnapshot.toLowerCase

        saveTumor(tumorid,finalsnapshot,year,branches)

        if ( printToScreen )
        {
          if ( year == 1.0 ) { ifprintln("year,branch,parent,mutation,beta,fitness,cells,mutations,dob,dod,population,maxpop,deaths") }
          for ( branch <- branches.indices ) {
            if ( branches(branch).cells > 0 || includeDeadBranches )  // graphing software needs to include even the dead branches
              ifprintln(year+","+branches(branch).subClone+","+branches(branch).parent+","+branches(branch).mutation+","+branches(branch).beta+","
                +branches(branch).fitness+","+branches(branch).cells+","+branches(branch).mutations+","+branches(branch).dob+","+branches(branch).dod
                +","+population+","+branches(branch).maxpop+","+branches(branch).deaths)
          }
        }
      }
    } while ( population >= minPopulation && population < maxPopulation && year < maxTumorGrowthYears )


    // save the chearleader, save the world -- its what any hero would do
    savePopulation(tumorid,populationHistory)

    // log final stats
    val finalYear = math.round(generation*3.0/366*10)/10d
    val quote = '"'
    println("tumorid= "+tumorid+"  final_generation= " + generation + "  years= " + finalYear + "  population= " + population)
    val saveTumorText = "saveTumor(tumorid="+quote+tumorid+quote+", finalsnapshot="+quote+"Y"+quote+",year="+finalYear+",tumor=tumor_"+tumorid+")"
    //println("to save the final snapshot manually:")
    //println(saveTumorText)

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
  // number of active classes at each level of completion;
  // ****************************************************************************************;

  def numberOfTumorClassesActive( tumorNum: Int) : Int =
  {
    if (tumorNum <    284)  6
    else if (tumorNum <    500)  3
    else if (tumorNum <  10000) 12
    else if (tumorNum <  58938)  8
    else if (tumorNum <  78500)  4
    else                         1
  }

  // ****************************************************************************************;
  // ****************************************************************************************;
  // ****************************************************************************************;

  def buildTumorid( tumorNum: Int , numberOfTumorClasses: Int , tumorSize : Int = 9 ) : String =
  {
    val numberOfTumorClasses =  numberOfTumorClassesActive( tumorNum )

    val tumorClass    = tumorNum % numberOfTumorClasses
    var lambdaClass   = "A"
    var mutationClass = "A"


    if (tumorNum < 284)
    {
      if (tumorClass == 1) { lambdaClass="A" ; mutationClass = "A" }
      if (tumorClass == 2) { lambdaClass="A" ; mutationClass = "B" }
      if (tumorClass == 3) { lambdaClass="A" ; mutationClass = "C" }

      if (tumorClass == 4) { lambdaClass="B" ; mutationClass = "A" }
      if (tumorClass == 5) { lambdaClass="B" ; mutationClass = "B" }
      if (tumorClass == 0) { lambdaClass="B" ; mutationClass = "C" }
    }
    else if (tumorNum < 500 )
    {
      if (tumorClass == 1) { lambdaClass="C" ; mutationClass = "A" }
      if (tumorClass == 2) { lambdaClass="C" ; mutationClass = "B" }
      if (tumorClass == 0) { lambdaClass="C" ; mutationClass = "C" }
    }
    else if (tumorNum < 10000)
    //    restart tumors at 500 with lambda_classes (A=0.1,B=0.01,C=0.005), and 4 mutation_classes (A=10^-8,B=10^-7,C=10^-6,D=10^-5)
    {
      if (tumorClass ==  1) { lambdaClass="A" ; mutationClass = "A" }    // AA${tumorSize}_${tumorNum}
      if (tumorClass ==  2) { lambdaClass="A" ; mutationClass = "B" }    // AB${tumorSize}_${tumorNum}
      if (tumorClass ==  3) { lambdaClass="A" ; mutationClass = "C" }    // AC${tumorSize}_${tumorNum}
      if (tumorClass ==  4) { lambdaClass="A" ; mutationClass = "D" }    // AD${tumorSize}_${tumorNum}

      if (tumorClass ==  5) { lambdaClass="B" ; mutationClass = "A" }    // BA${tumorSize}_${tumorNum}
      if (tumorClass ==  6) { lambdaClass="B" ; mutationClass = "B" }    // BB${tumorSize}_${tumorNum}
      if (tumorClass ==  7) { lambdaClass="B" ; mutationClass = "C" }    // BC${tumorSize}_${tumorNum}
      if (tumorClass ==  8) { lambdaClass="B" ; mutationClass = "D" }    // BD${tumorSize}_${tumorNum}

      if (tumorClass ==  9) { lambdaClass="C" ; mutationClass = "A" }    // CA${tumorSize}_${tumorNum}
      if (tumorClass == 10) { lambdaClass="C" ; mutationClass = "B" }    // CB${tumorSize}_${tumorNum}
      if (tumorClass == 11) { lambdaClass="C" ; mutationClass = "C" }    // CC${tumorSize}_${tumorNum}
      if (tumorClass ==  0) { lambdaClass="C" ; mutationClass = "D" }    // CD${tumorSize}_${tumorNum}
    }
    else if (tumorNum < 58938)
    //    restart tumors at 10000 with lambda_classes (A=skip,B=0.01,C=0.005), and 4 mutation_classes (A=10^-8,B=10^-7,C=10^-6,D=10^-5)
    {
      if (tumorClass ==  1) { lambdaClass="B" ; mutationClass = "A" }    // BA${tumorSize}_${tumorNum}
      if (tumorClass ==  2) { lambdaClass="B" ; mutationClass = "B" }    // BB${tumorSize}_${tumorNum}
      if (tumorClass ==  3) { lambdaClass="B" ; mutationClass = "C" }    // BC${tumorSize}_${tumorNum}
      if (tumorClass ==  4) { lambdaClass="B" ; mutationClass = "D" }    // BD${tumorSize}_${tumorNum}

      if (tumorClass ==  5) { lambdaClass="C" ; mutationClass = "A" }    // CA${tumorSize}_${tumorNum}
      if (tumorClass ==  6) { lambdaClass="C" ; mutationClass = "B" }    // CB${tumorSize}_${tumorNum}
      if (tumorClass ==  7) { lambdaClass="C" ; mutationClass = "C" }    // CC${tumorSize}_${tumorNum}
      if (tumorClass ==  0) { lambdaClass="C" ; mutationClass = "D" }    // CD${tumorSize}_${tumorNum}
    }
    else if (tumorNum < 78500)
    //    restart tumors at 58938 with lambda_classes (A=skip,B=skip,C=0.005), and 4 mutation_classes (A=10^-8,B=10^-7,C=10^-6,D=10^-5)
    {
      if (tumorClass ==  1) { lambdaClass="C" ; mutationClass = "A" }    // CA${tumorSize}_${tumorNum}
      if (tumorClass ==  2) { lambdaClass="C" ; mutationClass = "B" }    // CB${tumorSize}_${tumorNum}
      if (tumorClass ==  3) { lambdaClass="C" ; mutationClass = "C" }    // CC${tumorSize}_${tumorNum}
      if (tumorClass ==  0) { lambdaClass="C" ; mutationClass = "D" }    // CD${tumorSize}_${tumorNum}
    }
    else if (tumorNum <  304800)
    //    restart tumors at 78500 with lambda_classes (A=0.1,B=skip,C=skip), and 1 mutation_classes (A=10^-8)
    {
      if (tumorClass ==  0) { lambdaClass="A" ; mutationClass = "A" }    // AA${tumorSize}_${tumorNum}
    }
   else if (tumorNum <  310000)
    //    restart tumors at 304800 with lambda_classes (A=0.1,B=skip,C=skip), and 1 mutation_class (B=10^-7)
    {
      if (tumorClass ==  0) { lambdaClass="A" ; mutationClass = "B" }    // AB${tumorSize}_${tumorNum}
    }
   else if (tumorNum <  320000)
    //    restart tumors at 310000 with lambda_classes (A=skip,B=skip,C=0.005), and 1 mutation_class (A=10^-8)
    {
      if (tumorClass ==  0) { lambdaClass="C" ; mutationClass = "A" }    // CA${tumorSize}_${tumorNum}
    }
   else 
    //    restart tumors at 320000 with lambda_classes (A=skip,B=skip,C=0.005), and 1 mutation_class (D=10^-5)
    {
      if (tumorClass ==  0) { lambdaClass="C" ; mutationClass = "D" }    // CD${tumorSize}_${tumorNum}
    }

    lambdaClass + mutationClass + tumorSize + "_" + tumorNum

  }

  // ****************************************************************************************;
  // key ingredients to build a tumor
  // ****************************************************************************************;

  def tumorKeys(tumorNum : Int , tumorSize : Int = 9 ) : (Int,Int,Double,Double,String)  = {

    val numberOfTumorClasses = numberOfTumorClassesActive( tumorNum )
    val tumorClass = tumorNum % numberOfTumorClasses
    val tumorid = buildTumorid( tumorNum , numberOfTumorClasses , tumorSize )
    var lambda = 0.1;
    var driver_mutation_rate = math.pow(10,-4);

    if (tumorNum < 284)
    {
      // tumorid
      if (tumorClass == 1) { lambda = 0.1   ; driver_mutation_rate = math.pow(10,-4) }  // AA9_#
      if (tumorClass == 2) { lambda = 0.1   ; driver_mutation_rate = math.pow(10,-5) }  // AB9_#
      if (tumorClass == 3) { lambda = 0.1   ; driver_mutation_rate = math.pow(10,-7) }  // AC9_#

      if (tumorClass == 4) { lambda = 0.005 ; driver_mutation_rate = math.pow(10,-4) }  // BA9_#
      if (tumorClass == 5) { lambda = 0.005 ; driver_mutation_rate = math.pow(10,-5) }  // BB9_#
      if (tumorClass == 0) { lambda = 0.005 ; driver_mutation_rate = math.pow(10,-7) }  // BC9_#
    }
    else if (tumorNum < 500 )
    {
      // starting with tumor# 284 add 50 more tumors for each of the next 3 classes
      if (tumorClass == 1) { lambda = 0.05 ; driver_mutation_rate = math.pow(10,-4) }   // CA9_#
      if (tumorClass == 2) { lambda = 0.05 ; driver_mutation_rate = math.pow(10,-5) }   // CB9_#
      if (tumorClass == 0) { lambda = 0.05 ; driver_mutation_rate = math.pow(10,-7) }   // CC9_#
    }
    else if (tumorNum < 10000 )
    //    restart tumors at 500 with 3 lambda_classes (A=0.1,B=0.01,C=0.005), and 4 mutation_classes (A=10^-8,B=10^-7,C=10^-6,D=10^-5)
    {
      if (tumorClass ==  1) { lambda=0.1 ; driver_mutation_rate = math.pow(10,-8) }    // AA${tumorSize}_${tumorNum}
      if (tumorClass ==  2) { lambda=0.1 ; driver_mutation_rate = math.pow(10,-7) }    // AB${tumorSize}_${tumorNum}
      if (tumorClass ==  3) { lambda=0.1 ; driver_mutation_rate = math.pow(10,-6) }    // AC${tumorSize}_${tumorNum}
      if (tumorClass ==  4) { lambda=0.1 ; driver_mutation_rate = math.pow(10,-5) }    // AD${tumorSize}_${tumorNum}

      if (tumorClass ==  5) { lambda=0.01 ; driver_mutation_rate = math.pow(10,-8) }    // BA${tumorSize}_${tumorNum}
      if (tumorClass ==  6) { lambda=0.01 ; driver_mutation_rate = math.pow(10,-7) }    // BB${tumorSize}_${tumorNum}
      if (tumorClass ==  7) { lambda=0.01 ; driver_mutation_rate = math.pow(10,-6) }    // BC${tumorSize}_${tumorNum}
      if (tumorClass ==  8) { lambda=0.01 ; driver_mutation_rate = math.pow(10,-5) }    // BD${tumorSize}_${tumorNum}

      if (tumorClass ==  9) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-8) }    // CA${tumorSize}_${tumorNum}
      if (tumorClass == 10) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-7) }    // CB${tumorSize}_${tumorNum}
      if (tumorClass == 11) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-6) }    // CC${tumorSize}_${tumorNum}
      if (tumorClass ==  0) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-5) }    // CD${tumorSize}_${tumorNum}
    }
    else if (tumorNum < 58938)
    //    restart tumors at 10000 with lambda_classes (A=skip,B=0.01,C=0.005), and 4 mutation_classes (A=10^-8,B=10^-7,C=10^-6,D=10^-5)
    {
      if (tumorClass ==  1) { lambda=0.01 ; driver_mutation_rate = math.pow(10,-8) }    // BA${tumorSize}_${tumorNum}
      if (tumorClass ==  2) { lambda=0.01 ; driver_mutation_rate = math.pow(10,-7) }    // BB${tumorSize}_${tumorNum}
      if (tumorClass ==  3) { lambda=0.01 ; driver_mutation_rate = math.pow(10,-6) }    // BC${tumorSize}_${tumorNum}
      if (tumorClass ==  4) { lambda=0.01 ; driver_mutation_rate = math.pow(10,-5) }    // BD${tumorSize}_${tumorNum}

      if (tumorClass ==  5) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-8) }    // CA${tumorSize}_${tumorNum}
      if (tumorClass ==  6) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-7) }    // CB${tumorSize}_${tumorNum}
      if (tumorClass ==  7) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-6) }    // CC${tumorSize}_${tumorNum}
      if (tumorClass ==  0) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-5) }    // CD${tumorSize}_${tumorNum}
    }
    else if (tumorNum < 78500)
    //    restart tumors at 58938 with lambda_classes (A=skip,B=skip,C=0.005), and 4 mutation_classes (A=10^-8,B=10^-7,C=10^-6,D=10^-5)
    {
      if (tumorClass ==  1) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-8) }    // CA${tumorSize}_${tumorNum}
      if (tumorClass ==  2) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-7) }    // CB${tumorSize}_${tumorNum}
      if (tumorClass ==  3) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-6) }    // CC${tumorSize}_${tumorNum}
      if (tumorClass ==  0) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-5) }    // CD${tumorSize}_${tumorNum}
    }
    else if (tumorNum <  304800)
    //    restart tumors at 78500 with lambda_classes (A=0.1,B=skip,C=skip), and 1 mutation_class (A=10^-8)
    {
      if (tumorClass ==  0) { lambda=0.01 ; driver_mutation_rate = math.pow(10,-8) } // AA${tumorSize}_${tumorNum}
    }
   else if (tumorNum <  310000)
    //    restart tumors at 304800 with lambda_classes (A=0.1,B=skip,C=skip), and 1 mutation_class (B=10^-7)
    {
      if (tumorClass ==  0) { lambda=0.01 ; driver_mutation_rate = math.pow(10,-7) } // AB${tumorSize}_${tumorNum}
    }
   else if (tumorNum <  320000)
    //    restart tumors at 310000 with lambda_classes (A=skip,B=skip,C=0.005), and 1 mutation_class (A=10^-8)
    {
      if (tumorClass ==  0) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-5) } // CA${tumorSize}_${tumorNum}
    }
   else 
    //    restart tumors at 320000 with lambda_classes (A=skip,B=skip,C=0.005), and 1 mutation_class (D=10^-5)
    {
      if (tumorClass ==  0) { lambda=0.005 ; driver_mutation_rate = math.pow(10,-5) } // CD${tumorSize}_${tumorNum}
    }

    ( numberOfTumorClasses , tumorClass , lambda , driver_mutation_rate , tumorid )
  }

// ****************************************************************************************;
// grow one specialized class tumor mod 6;  tumorSize is medically identifyable at 10^9 cells but adjustable for testing
// ****************************************************************************************;

  def growOneSpecializedTumor( tumorNum : Int , numberOfTumorClasses : Int = 6 , tumorSize: Int = 9 ) : ArrayBuffer[branch] =  // returns the full tumor in case we want to manipulate, browse, clone, etc...
  {
    val maxPopulation = math.pow(10,tumorSize).toInt

    var ( numberOfTumorClasses, tumorClass , lambda , driver_mutation_rate , tumorid ) = tumorKeys( tumorNum )

    // println("Working on:  tumorid="+tumorid+", maxPopulation="+maxPopulation)

    growTumor( tumorid , lambda , driver_mutation_rate , maxPopulation )

  }

  // ****************************************************************************************;
  // parallel save multiple tumors in a permanent parquet table on hdfs
  // ****************************************************************************************;

  def parallelTumors( start : Int =  1 , end : Int = 1  ) : Unit =
  {
    // parallel version not working - org.apache.spark.SparkException: Task not serializable
    //   sc.parallelize(start to end , /* core_slices = */ 10 ).map(t => growOneSpecializedTumor( t ))

    // future concurent parallelized
    for (t <- start to end ) Future { growOneSpecializedTumor( t ) }
  }

  // ****************************************************************************************;
  // sequential save multiple tumors in a permanent parquet table on hdfs
  // ****************************************************************************************;

  def sequentialTumors( start : Int =  1 , end : Int = 1 ) : Unit =
  {
    for (t <- start to end ) growOneSpecializedTumor( t )
  }

  // ****************************************************************************************
  // remove an partial tumor due to mid gen crash
  // ****************************************************************************************

  def removeTumor( tumorNum : Int , numberOfTumorClasses : Int = 6 , tumorSize: Int = 9 ) : Unit =
  {
    val tumorid = buildTumorid(tumorNum, numberOfTumorClasses, tumorSize)
    //hc.sql("alter table tumors      drop if exists partition (tumorid='"+tumorid+"')")
    //hc.sql("alter table populations drop if exists partition (tumorid='"+tumorid+"')")
  }

  // ****************************************************************************************
  // initialize hive metastore
  // ****************************************************************************************

  def  initializeHive()
  {
    // due to authorization failure, tried to override HiveServer2-site.xml, but security will not allow it
    //hc.sql("set hive.security.authorization.enabled=true")
    //hc.sql("set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider")
    //hc.sql("set hive.exec.failure.hooks=")  // default is org.apache.hadoop.hive.ql.hooks.ATSHook
    //hc.sql("set hive.exec.pre.hooks=")      // default is org.apache.hadoop.hive.ql.hooks.ATSHook
    //hc.sql("set hive.exec.post.hooks=")     // default is org.apache.hadoop.hive.ql.hooks.ATSHook
    //hc.sql("set hive.cli.print.header=true") // turn on column headers for pretty printing only
    //hc.sql("set hive.execution.engine=tez") // default is mr, current hive implementation allows for [mr,tez], spark not allowed in this version

    //hc.sql("create database if not exists tumorsim location '/project/tumorsim'") // authorization failure
    // authorization failure from create database statement
    // FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory.createHiveAuthorizer(Lorg/apache/hadoop/hive/
    // ql/security/authorization/plugin/HiveMetastoreClientFactory;Lorg/apache/hadoop/hive/conf/HiveConf;Lorg/apache/hadoop/hive/ql/security/HiveAuthenticationProvider;)Lorg/apache/hadoop/hive/ql/security/authorization/plugin/HiveA
    // uthorizer;
    // run a second time and it works
    //hc.sql("create database if not exists tumorsim location '/project/tumorsim'") // success on second attempt

    //hc.sql("use tumorsim")

    //hc.sql("create table if not exists tumorSim.tumors ( subClone Int , parent Int , mutation Double , beta Double , fitness Double , cells Int , mutations Int , dob Int , dod Int , maxpop Int , deaths Int , mutation_selection Double , resistant Int) partitioned by ( tumorid String , finalsnapshot String , year double ) stored as parquet")
    //hc.sql("create table if not exists tumorSim.populations ( generation Int , population Int , mass10to7th Int , mass10to8th Int) partitioned by ( tumorid String ) stored as parquet")

    // can access a subset via hive with limit statement, but faster would be take(8).collect()
    // hc.sql("FROM tumors SELECT * limit 8").collect().foreach(println)
    // or
    // hc.sql("FROM tumors SELECT * ").take(8).collect().foreach(println)
    // combine both limit and take?
  }

  // ******************************************************************************************************************************************************
  // load a previously created tumor file for one tumor
  // ******************************************************************************************************************************************************

  def loadTumor( tumorNum : Int = 75064 ) : ArrayBuffer[branch] =
  {
    var branches = new ArrayBuffer[branch]()

    val importPath = "/project/tumorsim/tumors.xls"
    val tumorClassWild = "*"
    val finalsnapshot = "Y"
    val finalYearWild = "*"
    val importPathFile  = importPath + "/" + tumorClassWild + "_" + tumorNum + "_" + finalsnapshot + "_" + finalYearWild + "_tumor.xls"

    println("Loading "+importPathFile)

    val text = sc.textFile(importPathFile)

    text.collect.foreach{ line =>

      val fields             = line.split("\\t")

      val subClone           = fields( 0).toInt
      val parent             = fields( 1).toInt
      val mutation           = fields( 2).toDouble
      val beta               = fields( 3).toDouble
      val fitness            = fields( 4).toDouble
      var cells              = fields( 5).toInt
      val mutations          = fields( 6).toInt
      val dob                = fields( 7).toInt
      var dod                = fields( 8).toInt
      var maxpop             = fields( 9).toInt
      var deaths             = fields(10).toInt
      var mutation_selection = fields(11).toDouble
      var resistant          = fields(12).toInt

      // println(s"subClone=$subClone parent=$parent  mutation=$mutation  beta=$beta  fitness=$fitness  cells=$cells  mutations=$mutations  dob=$dob  dod=$dod  maxpop=$maxpop deaths=$deaths  mutation_selection=$mutation_selection  resistant=$resistant")

      val nextBranch = new branch( subClone,parent,mutation,beta,fitness,cells,mutations,dob,dod,maxpop,deaths,mutation_selection,resistant)

      branches += nextBranch
    }
    branches  // yeild
  }

  /* heres how to verify the tumor was loaded correctly:

    val branches = loadTumor( 75064 )

    // see full tumor loaded
    for (branch <- 0 to branches.size-1)
        println(branches(branch).subClone+","+branches(branch).parent+","+branches(branch).mutation+","+branches(branch).beta+","
                  +branches(branch).fitness+","+branches(branch).cells+","+branches(branch).mutations+","+branches(branch).dob+","+branches(branch).dod
                  +","+branches(branch).maxpop+","+branches(branch).deaths)

    // - or a smaller sample -
    for (branch <- 0 to 2)
        println(branches(branch).subClone+","+branches(branch).parent+","+branches(branch).mutation+","+branches(branch).beta+","
                  +branches(branch).fitness+","+branches(branch).cells+","+branches(branch).mutations+","+branches(branch).dob+","+branches(branch).dod
                  +","+branches(branch).maxpop+","+branches(branch).deaths)

        0,-1,0.008055410844588754,0.9919445891554113,1.0080554108445887,6372362,1,1,0,6372428,394440342
        1,0,0.0038359511866410856,0.9881395381315584,1.0118604618684417,0,2,567,600,16,104
        2,0,0.007864706972184042,0.9841432356290605,1.0158567643709395,0,2,753,754,1,1
  */

  // ******************************************************************************************************************************************************
  // load a previously created population file for one tumor
  // ******************************************************************************************************************************************************

  def loadPopulation( tumorNum : Int = 75064 ) : ArrayBuffer[genPop] =
  {
    var populationHistory = new ArrayBuffer[genPop]()

    val importPath = "/project/tumorsim/populations.xls"
    val tumorClassWild = "*"
    val importPathFile  = importPath + "/" + tumorClassWild + "_" + tumorNum + "_population.xls"

    println("Loading "+importPathFile)

    val text = sc.textFile(importPathFile)

    text.collect.foreach{ line =>

      val fields             = line.split("\\t")

      val generation         = fields( 0).toInt
      val population         = fields( 1).toInt
      val mass10to7th        = fields( 2).toInt   // track in population history indicating found branch with 10^7 cells at each generation
    val mass10to8th        = fields( 3).toInt   // track in population history indicating found branch with 10^8 cells at each generation

      // println(s"generation=$generation population=$population  mass10to7th=$mass10to7th  mass10to8th=$mass10to8th")

      val nextGeneration = new genPop( generation,population,mass10to7th,mass10to8th )  // track population at each generation

      populationHistory += nextGeneration
    }
    populationHistory  // yeild
  }


  /* heres how to verify the population was loaded correctly:

    val pop = loadPopulation( 75064 )

    // see full population file loaded
    for (gen <- 0 to pop.size-1) println(pop(gen).generation+","+pop(gen).population+","+pop(gen).mass10to7th+","+pop(gen).mass10to8th)

    // - or a smaller sample -
    for (gen <- 0 to 2) println(pop(gen).generation+","+pop(gen).population+","+pop(gen).mass10to7th+","+pop(gen).mass10to8th)
        1,1,0,0
        2,2,0,0
        3,2,0,0

    println(pop(pop.size-1).generation)
      1698

    println(pop.size)
      1698
  */


  // ************************************************************************************************************************
  // load previously detectable tumor and population history, apply chemo to largest subclones, then restart growth until second detection
  // ************************************************************************************************************************

  def applyChemo( tumorNum           : Int    = 75064
                  , chemoThreshold     : Double = 0.05*math.pow(10,9)            // default chemo at 5% detectable or 0.05*10^9 cells
                  , tumorSize          : Int    = 9                              // original detetable size of 10^9 cells
                  , generationsPerYear : Int    = 122                            // 3 days per generation by default
                  ) : Unit =
  {

    // key tumor encoding
    var ( numberOfTumorClasses , tumorClass , lambda , driver_mutation_rate , tumorid ) = tumorKeys( tumorNum , tumorSize )

    // load tumor and population history
    val branches              = loadTumor      ( tumorNum )
    val populationHistory     = loadPopulation ( tumorNum )

    // determine previous final generation and population size
    val maxPopulation         = math.pow(10,tumorSize).toInt
    var generation            = populationHistory(populationHistory.size-1).generation + 1  // use up one generation for chemo
  var population : Int      = 0                                                           // recalc from known tumor below
  var origPop : Int         = 0                           // track for before/after stats
  var mass10to7th : Int     = 0
    var mass10to8th : Int     = 0

    // chemo applied before regrowth to wipe out all non-resistant subclones
    for (branch <- 0 to branches.size-1) {
      origPop += branches(branch).cells     // orignal population before chemo
      if ((branches(branch).cells > 0) && (branches(branch).resistant == 0)) {
        branches(branch).cells = 0 // kill off all subclones over a certain size
        branches(branch).dod = generation // date of clonal death from chemo

        if (branches(branch).cells >= math.pow(10, 7).toInt) mass10to7th += 1 // how many branches with 10^7 cells at each generation
        if (branches(branch).cells >= math.pow(10, 8).toInt) mass10to8th += 1 // how many branches with 10^8 cells at each generation
      } else population += branches(branch).cells // remaining population
    }

    println(s"OrigPop=$origPop,  NewPop=$population,  generation=$generation,  tumorClass=$tumorClass, tumorId=$tumorid, lambda=$lambda, driver_mutation_rate=$driver_mutation_rate")

    // save a chemo snapshot of the tumor and track the new population in this chemo generation
    populationHistory += genPop(generation,population,mass10to7th,mass10to8th)    // population tracked after chemo applied

    var year = math.round(generation/generationsPerYear*10)/10.0     // equivalent to 3 days per generation by default
  var finalsnapshot : String = "c"                                 // chemo snapshot
    saveTumor(tumorid,finalsnapshot,year,branches)                   // save the tumor after chemo applied

    // restart growth until detection
    growTumor( tumorid , lambda , driver_mutation_rate , maxPopulation , branches , populationHistory , population )

  }

  // applyChemo( 75064 )
  //    OrigPop=1007282996,  NewPop=16082730,  generation=1698,  tumorClass=0, tumorId=CD9_75064, lambda=0.005, driver_mutation_rate=1.0E-5

  // ****************************************************************************************
  // main function needs to initialize hive metastore and grow one or more tumors
  // ****************************************************************************************

  def main ( args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: Tumor <del|add|mult|repair> <startTumor#> [<endTumor#>]")
      System.exit(1)
    }
    // initializeHive()  // only needed if we passed some parms , turned metadata updates off
    args(0) match {
      case "add"        => Future { growOneSpecializedTumor( args(1).toInt ) }
      case "del"        => Future { removeTumor( args(1).toInt ) }
      case "parallel"   => parallelTumors( start = args(1).toInt , end = args(2).toInt )
      case "sequential" => sequentialTumors( start = args(1).toInt , end = args(2).toInt )
      case "repair"     => Future { removeTumor( args(1).toInt ) ; growOneSpecializedTumor( args(1).toInt ) }
      case "chemo"      => applyChemo( args(1).toInt )
      case _ =>  println("Usage: <add|del|parallel|sequential|repair|chemo> <start#> [<mult_end#>]")  // anything else looks like help
    }
  }
}

// *** manual method to add/del a single tumor
//Tumor.main ("del 284")
//Tumor.main ("add 284")
//Tumor.main ("parallel 1001 6001")
//Tumor.main ("repair 2012")
//Tumor.main ("repair 1826")
//Tumor.main("help")      // run arguments passed via environment variable $tumorargs
