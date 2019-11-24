# Amedeo Baragiola - HW3 CS441
#### Politecnico di Milano
#### University of Illinois at Chicago

## Introduction

This homework consists in making multiple stock trading Monte Carlo simulations given historical data on the stock trends.
The Spark framework has been extensively used as part of this homework.

## Requirements

* HDP Sandbox (https://www.cloudera.com/downloads/hortonworks-sandbox.html) imported in VMWare. Please read the documentation present on the cloudera.com website for instruction on how to set up and configure the Sandbox.
* A data connection to download historical stock data using the Alpha Vantage APIs.
* ssh and scp installed and working on the host system.
* Java JDK 8 installed and working on the host system. (OpenJDK and Oracle are both supported)

### Test setup

* HDP Sandbox
* MacOS High Sierra
* VMware Fusion 11.5
* merged.csv file included in the repository. (This is useful to replicate results, however, the code for creating such file from the API responses is of course included in the project)

## Installation instructions
This section contains the instructions on how to run the simulations implemented as part of this homework, the recommended procedure is to use IntellJ IDEA with the Scala plugin installed.

1. Open IntellJ IDEA, a welcome screen will be shown, select "Check out from Version Control" and then "Git".
2. Enter the following URL and click "Clone": https://bitbucket.org/abarag4/amedeo_baragiola_hw3.git
3. When prompted confirm with "Yes"
4. The SBT import screen will appear, proceed with the default options and confirm with "OK"
5. Confirm overwriting with "Yes"
6. You may now go to src/main/scala/com.abarag4/ where you can find the two main simulation classes: StockSimulator and StockSimulatorParallelize. A run configuration is automatically created when you click the green arrow next to the main method of the driver class. An additional main class is present: GetStockTimeSeries which retrieves the data from the API and generates the merged.csv file.

**Important note about running the project:** When running the project locally, make sure that the "local" flag is set to true in the configuration.conf file under resources/, otherwise the code will NOT run.

Note: All the required dependencies have been included in the build.sbt and plugins.sbt files. In case dependencies can't be found, check that the maven URLs are still current. Be aware that this project uses scala 2.11.8 with Spark 2.4.0 as specified in the build.sbt file.
Note (input data): The input .xml file needs to be copied in the input_dir folder, otherwise the job execution will fail.

#### Alternative: SBT from CLI

If you don't want to use an IDE, you may run this project from the command line (CLI), proceed as follows:

1. Type: git clone https://bitbucket.org/abarag4/amedeo_baragiola_hw3.git
2. Before running the actual code, you may wish to run tests with "sbt clean compile test"
3. Run the code: "sbt clean compile run"

Note: "sbt clean compile run" performs a local execution of the map/reduce jobs. The jobs are run in sequence, you may wish to read the next section on how to assemble an actual jar.
Note (input data): The input .xml file needs to be copied in the input_dir folder, otherwise the job execution will fail.

#### Creating a JAR file

You may wish to create a Jar file so you can run the map/reduce jobs on AWS EMR or on a virtual machine that provides an Hadoop installation, proceed as follows:

1. Type: git clone https://bitbucket.org/abarag4/amedeo_baragiola_hw3.git
2. Run the following command: "sbt clean compile assembly"
3. A Jar will be created in the target/scala-2.11/ folder with the following name: amedeo\_baragiola\_hw3.jar.

Note: Make sure that the Java version present on the machine you compile the code on is older or matches the version on the machine you run it on. Java JDK 1.8 is recommended. More recent Java versions are NOT supported.

## Deployment instructions (HDP Sandbox)

You may wish to deploy this homework on the HDP Sandbox in order to run it and test its functionality. Start by creating a Jar file as outlined in the previous section, then proceed as follows:

**Important note about running the code:** Make sure you have set the "local" flag to false in the configuation.conf file under resources/ before running the project in cluster mode.

1. Make sure the HDP Sandbox is running and it has fully started. This make take a while even on a powerful machine. You may check by logging in the web panel and checking the status on the "Start services" task.
2. Copy the jar file to the Sandbox by issuing the following command: "scp -P 2222 target/scala-2.11/amedeo\_baragiola\_hw3.jar root@sandbox-hdp.hortonworks.com:/root"
3. Copy the dblp.xml to the Sandbox: "scp -P 2222 dblp.xml root@sandbox-hdp.hortonworks.com:/root"
4. Login into the Sandbox: ssh -p 2222 -l root sandbox-hdp.hortonworks.com. You may be asked for a password if you have not set up SSH keys. The default password is: hadoop
5. Create the input directory on HDFS: hdfs dfs -mkdir input_dir
6. Load the dataset on HDFS: hdfs dfs -put merged.csv input_dir/
7. You can now launch the job: spark-submit --verbose --deploy-mode cluster amedeo\_baragiola\_hw3.jar. (Note: You are deploying in cluster mode, check that the "local" flag in configuration.conf is set to false!)
8. After completion the results are saved in a folder named output_dir on HDFS, you can copy them to local storage by issuing the following command: "hdfs dfs -get output_dir output"
9. Exit from the SSH terminal, "exit"
10. Copy the results to the host machine: scp -P 2222 -r root@sandbox-hdp.hortonworks.com:/root/output <local_path>

Note: Instead of just copying the data from HDFS, I recommend merging the output files.
In order to do that, issue the following command: "hadoop fs -getmerge /output_dir/job_specific_dir/ <local_path>.
You may list job specific output dirs by issuing: "hdfs dfs -ls output_dir".

Additional note: Depending on your DNS settings on the your host machine the hostname sandbox-hdp.hortonworks.com may fail to resolve, if this happens you can either add it to /etc/hosts or use the IP address of the Sandbox instead in the commands above.

## Spark implementations

This homework is about creating Monte Carlo simulations of trades on the stock market for a randomly chosen list of stocks. Being Monte Carlo implies that we ru multiple simulations each with a random component; Monte Carlo algorithms use multiple runs as a sort of validation of the result they obtain.

Intuitively, by performing an operation a certain number of times the result obtained is more credible than doing the same only once.

Two main approaches were followed, a first one (1) in which the multiple simulations are run in parallel, but the input data is replicated in its entirety on the different worker machines. (each runs a separate simulation on the full data set) and a second one (2) in which the simulations are run sequentially, but the input data set is distributed across multiple machines instead of being fully replicated.

Both approaches aim at showing different aspects of running a Spark simulation and both make sense given the problem settings and are applicable to the respective real-world scenarios. In this specific context approach 1 seems the most sensible given the size of the input data.

### Parallelizing the simulations

Approach (1) is better when the input data size is relatively small, like in this instance where the full 20-years historical data of the various trends is only about 70MB in size. However, with a large dataset, it would quickly become impractical as the data wouldn't fit on a single machine. (even when it fits, the results in terms of speed may be suboptimal) This is when Approach (2) comes into play.

This approach is implemented in "StockSimulatorParallelize.java", which is run and compiled by default for this project.

### Distributing the input data

Approach (2) also aims at showing an extensive usage of RDDs to manipulate distributed data using the Spark framework. The input data is distributed and split among multiple machines, but the simulations are done sequentially.

This approach is implemented in "StockSimulator.java", this file must be set as the Main class in build.sbt in order to be able to assemble a jar and run the project with this approach.

## Policy implementation

In order to perform simulations on time series data like the one provided a simulation policy needs to be defined and implemented.

As it is commonly known the stock market is unpredictable and stock values can change suddently without notice. Timing the stock market in an effort to make better predictions usually results in money losses.

That said, and having clear that a perfect policy doesn't exist, the one implemented here aims at achieving two main goals: Stopping the losses and selling stocks with stable trends.

The implemented policy is executed for every day in the dataset and based on the trend of the stocks, an action to buy/sell certain stocks can be taken.

Furthermore, the implemented policy supports fractional stock purchases, this means  that whenever we don't have the full money amount to buy an integer number of units of a certain stock, we can still buy a fraction of the stock we are interested in.

### (a) Stopping losses

The policy, each time it is run, checks whether each stock in the current portfolio has decreased in value of more than a certain amount delta.
If so, the stock is sold in full and another random stock is bought to replace it.

### (b) Setting stocks with stable trends

The policy, each time it is run, checks whether each stock in the current portfolio has maintained its value within a certain delta. If so, the stock is sold in full and another random stock is bought to replace it.

### Buying new stocks to replace the ones currently owned

When a decision is made to sell a certain stock due to condition (a) or (b) a new stock needs to be selected for purchase. This is the random component of this policy; the stock is randomly selected among those not currently owned and if that is not possible (all stocks are already owned), it is randomly selected among all possible stocks.


## Creating charts

Upon completion of the Spark simulations the output is produced in csv format. The default output directory is output_dir on the HDFS filesystem.
If you have previously merged the output files (recommended) you shall now have a single output file. You are required to name the files appropriately.

From now on, we assume that the files will be named out.csv.
If different names are used changes will need to be made in the relevant files.

### Requirements to plot charts

* Python notebooks (Jupyter or JupyterLab installed. Installation instructions here: https://jupyterlab.readthedocs.io/en/stable/getting_started/installation.html
* Pandas library for Python notebooks. Instructions here: https://pandas.pydata.org/pandas-docs/stable/install.html
* PyPlot for Python notebooks. Instructions here: https://matplotlib.org/3.1.1/users/installing.html

In order to create charts an industry-standard tool in the data science world has been used: Pandas.
A Python notebook with the relevant code can be found in the "HW3.ipynb" file in the repository charts/ folder, the following instructions explain how to run it:

1. Make sure that the requirements listed above have been installed.
2. Open Jupyter or JupyterLab and drag the HW3.ipynb to open it.
3. Copy the output files from the map/reduce jobs in the same folder as the .ipynb file.
4. Click on "Run" -> "Run all cells".

The charts/ folder in the repository root also contains the charts obtained by following the procedure above.

Note: Make sure that the file names match those listed in the python notebook source code, if you use different names you may need to change them.

Demo URL: https://amedeobaragiola.me/HW3.html

## AWS EMR Deployment

A YouTube video showing the AWS EMR deployment process is available here: https://www.youtube.com/watch?v=NwX04rRdOdo


