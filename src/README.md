# yelp-help-supreme

Assumption:
  - You atleast have spark setup and can run our hdfs, otherwise have your own hdfs up as well

Steps to run our code:
  - Available clusters to run the code:
    * HDFS
    - Use our hadoop cluster at salem:42201 / Web UI - salem:42202/yelp
    - Use : des-moines:42850 / Web UI - des-moines:42801
    * SPARK
    - kinshasa:7077 / Web UI - kinshasa:8080
    - tokyo:7077 / Web UI - tokyo:8080

  OR

  - Download the Yelp Dataset only (excluding photos) at: https://www.yelp.com/dataset_challenge
  - Extract it and put these JSON files on to your hadoop cluster
    - yelp_academic_dataset_review.json
    - yelp_academic_dataset_user.json

First command to run:
  - cd into the folder where you extracted the project
  - root folder is where you can see the src folder
  - run: mvn clean compile package
    - this will create classes and a jar file

Commands to run the code:
  Classes available:
    - AssignEliteAttributes
    - BusinessMonthlyRating
    - BusinessYearRating
    - InfluencePoints
    - MLearning
    - MonthlyInfluencePoints

How to increase jobs:
  - cd into $SPARK_HOME/conf
  - Create a file called: spark-defaults.conf
  - Write a line: spark.executor.memory 3g
  - Save and exit

Command to execute classes:
    - Change "CLASS-NAME" to one of the above
      - include the quotations
    - Change <SPARK-MASTER> to either spark://kinshasa:7077 or spark://tokyo:7077
      - or to your spark master node (exclude the angled brackets)
    - $SPARK_HOME/bin/spark-submit --class "CLASS-NAME" --master <SPARK-MASTER> target/YelpHelpSupreme-0.0.1.jar

Order to run: 
  - BusinessMonthlyRating.java or BusinessYearRating.java
  - InfluencePoint.java or MonthlyInfluencePoints.java or NormalInfluencePoints.java
  - AssignEliteAtrributes.java
  - MLearning.java, MLExtremeScale.java, TestScaledExtreme.java, TestScaledExtremeScaledAtts.java, TestScaledScale.java, TestScaledScaledAtts.java, testML.java, or testML2.java
  Note: File paths and certain comments may need to be changed to run different Machine learning analytics
    
Java files:
  - BusinessYearRating.java
    - This creates a json file containing each restaurant's average review per year
      from the selected attributes of the yelp_academic_dataset_review.json file.
    - Mapper: restaurant_id + year is the key and the stars and number of reviews as values
  - BusinessMonthlyRating.java
    - This creates a json file containing each restaurant's average review per year per month
      from the selected attributes of the yelp_academic_dataset_review.json file.
    - Each Buisness_id is concated with an monthYear integer for each month year combination.
    - This will produce a file RestaurantPerMonthlyYearRating.json that contains the Monthly rating for each Restaurant.
  - InfluencePoint.java
    -
  - MonthlyInfluencePoints.java
    -
  - NormalInfluencePoints.java
    -
  - AssignEliteAtrributes.java
    -
  - MLearning.java
    -
  - MLExtremeScale.java
    -
  - TestScaledExtreme.java
    -
  - TestScaledExtremeScaledAtts.java
    -
  - TestScaledScale.java
    -
  - TestScaledScaledAtts.java
    -
  - testML.java
    -
  - testML2.java
    -
