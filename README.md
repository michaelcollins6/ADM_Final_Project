# ADM_Final_Project

Steps to Run the Application

1. Open the terminal in administrator mode
2. Start the Hadoop cluster by running the "start-all" command in the terminal
  start-all.cmd

3. Verify all daemons are working by running the "jps" command

4. Submit the Spark job by using the "spark-submit" command
  spark-submit --master yarn --deploy-mode client --num-executors 2 --executor-cores 2 --executor-memory 2g *path-to-file

5. Verify using the Hadoop UI web page
  http://localhost:9870/
6. Provide an input and set the filters as necessary 
  (ex: Los Angeles or 33.0, -118.0)
7. View the results. Step 6 can be repeated as needed without closing the program.
8. End the cluster by running the "stop-all" command
  stop-all.cmd
