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
  - AssignEliteAtrributes.java, TestScaledExtreme.java, TestScaledExtremeScaledAtts.java, or TestScaledScale.java, TestScaledScaledAtts.java
  - MLearning.java, MLExtremeScale.java, testML.java, or testML2.java
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
    - Calcuates Average InfluencePoints for each elite user with yearly Business rating data.
    - An Influence Point can range from 0 to 1. This defines how a elite user review rating effects future rating for that restaurnt.
  - MonthlyInfluencePoints.java
    - Calcuates Average Infleunce Points for each elite user or normal user with monthly Business rating data..
    - A normal user is a user that has never been given the elite status.
  - NormalInfluencePoints.java
    - Calcuates Average InfluencePoints for each normal user with yearly Business rating data.
    - A normal user is a user that has never been given the elite status
  - AssignEliteAtrributes.java
    - For each user (Elite or Normal) pulled from a json file it gets the attriubtes for that user and outputs it into a libsvm formatted file for Machine learning
    - Comment out attributes to limited the attributes used.
    - Infleunce points are put on 5 points scale and distributed evenly to each category. See file for more information of scaling.
  - TestScaledExtreme.java
    - Simlar to AssignEliteAttributes, gives users 19 attributes but their influence is created based on a higher scale: 0 =     
    <.025, 1 = < .05, 2 = <.075... A deeper scale to map users to a different influence scale. Attempted this to help machine 
    learning models map to different categories.
  - TestScaledExtremeScaledAtts.java
    - Similar to AssignEliteAttributes, but all attributes are divided by the number of reviews a user has made, depending on the 
    number returned by the att/numreviews, we create a new scale for the attribute, giving a number between 0-20 as the 
    attribute. This was an attempt that if the attributes are scaled between similar numbers that Machine Learning trend can be 
    developed, the more scales the easier it is ascertain the category a user falls in.
  - TestScaledScale.java
    - Similar to AssignEliteAttributes, but all attributes are divided by the number of reviews a user has made. Difference from 
    TestScaledExtremeScaledAtts.java -> The value att/numReviews were not scaled. 
  - TestScaledScaledAtts.java
    - Similar to AssignEliteAttributes, but all attributes are divided by the number of reviews a user has made. The value is that 
    put on a scale that would return 0 -9. Difference from TestScaledExtremeScaledAtts.java -> The scale placed is smaller. 
    Thought behind it was to put attributes on a scale for machine learning to easily create a trend.
  - MLearning.java
    - Sample format for Multilayer Perceptron Classification Machine Learning tests.
    - Select training data, and determines test set accuracy for the given model.
  - testML.java
    - Runs a loop on the two models that we ran our data through. The ArrayList diffLayers were changed on different hidden layers, high and low, more or less layers. Int[] nIterations added more iterations, from what we've seen, they all converge to one similar output after x iterations. Inputs a .txt written in libsvm format from hdfs cluster of data that you wish to run through machine learning model. Outputs a .txt file specified
  - testML2.java
    - TestML2 is similar to testML. The information for diffLayers and int[] nIterations were changed and specifed for the test that we wanted to run on x.txt file and output as y.txt file
