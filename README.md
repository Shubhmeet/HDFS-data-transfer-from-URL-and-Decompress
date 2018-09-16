# HDFS-data-transfer-from-URL-and-Decompress
* Implemented (in Java) data transfer directly from URL to URI (HDFS cluster) without using any intermediate storage used. 
* Locally decompressed the downloaded .bz2/.zip(compressed) file using CompressionCodecFactory and ZipInputStream API. 
* Application runs on command line using the .Jar package of the Java project created earlier stored locally on Hadoop cluster.

## Steps for execution of Part-1 and Part-2 

  1) Import the "Part1" project as "Existing Project" in IntelliJ.
  2) Build the Project by clicking on view->Tool Windows-> Maven Projects. 
     Then you will see Maven project named Assignment1. click on Lifecycle -> double click on package to build the maven project.
     This should display Build Success.
  3) If it builds successfully, you will get a jar file in the "target" folder of your project namely "Assignment1-1.0-SNAPSHOT.jar".
  4) Now click on Tools -> Deployment -> Browse Remote Host.
     If you dont have remote host already created, need to create one.
     Creating remote host:
    -> Name your Remote host and with type SFTP.
    -> SFTP host : csgrads1.utdallas.edu
    -> Root path -> click on Autodetect 
    -> Enter your credentials.
    -> Click ok.
  5) You can then upload the "JavaHDFS-0.0.1-SNAPSHOT.jar" to the cluster using SFTP client created in step 4. 
     (i.e., copy the file from Local to Remote Connection)
  6) Run the jar file on the cluster by the following command:

## Running Part1 of assignment :

  * hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar Part1 hdfs://cshadoop1/user/<net_id>/
  * Enter the following command "hdfs dfs -ls" to see the decompressed .txt files on hadoop cluster.
  * Enter the following command "hdfs dfs -cat <name>.txt" to see the contents of text file.

## Running Part2 of assignment :

  * Enter the following command to run Part 2: hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar Part2 srcUrl hdfs://cshadoop1/user/<userId>/
    eg: hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar Part2 http://corpus.byu.edu/glowbetext/samples/database.zip     hdfs://cshadoop1/user/sxk162731/
   * Enter the following command " hdfs dfs -ls " to see the decompressed .txt files on hadoop cluster. 
   * The contents of the zip file is copied into the Hadoops File system. In our example it will be text.txt.
check the contents of the txt file using : "hdfs dfs -cat <text file Name>"
					  eg: "hdfs dfs -cat text.txt"
