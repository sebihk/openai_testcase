# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 21:07:14 2024

@author: WuJia
"""
from pyspark.sql import SparkSession, Row
import pydeequ
from pydeequ.analyzers import *
from pydeequ.suggestions import *
from pydeequ.checks import *
from pydeequ.verification import *
import os
import pandas as pd
#from ydata_profiling import ProfileReport, compare
from ydata_profiling import ProfileReport, compare
import great_expectations as gx
os.environ['SPARK_VERSION'] = '3.3'
os.environ['SPARK_HOME'] = 'C:/work73/war/RiskConfidence/spark'
os.environ['HADOOP_HOME'] = 'C:/work73/war/RiskConfidence/spark'



def get_constraint_recommend ( path, filename):
    spark = (SparkSession
       .builder
       .config("spark.jars.packages", pydeequ.deequ_maven_coord)
       .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
       .config("spark.executor.heartbeatInterval", "100s")
       .getOrCreate())

    df = spark.read.options(delimiter=",", header=True).csv( os.path.join(path, filename))


    suggestionResult = ConstraintSuggestionRunner(spark) \
             .onData(df) \
             .addConstraintRule(DEFAULT()) \
             .run()
    
         
    with open(os.path.join(path, filename+'.json'), 'w' ) as f:
        json.dump(suggestionResult, f ,indent=2  , sort_keys=True,
                            ensure_ascii=False )
    return (  filename+'.json')  

def gen_data_profile(  path, filename):
    df = pd.read_csv( os.path.join(path, filename))
    profile_name=os.path.join(path, filename+'.html')
    profile = ProfileReport(df, title=filename+"Profiling Report")
    profile.to_file(profile_name)
    return (filename+'.html')
           
# Constraint Suggestions in JSON format
"""
analysisResult = AnalysisRunner(spark) \
.onData(df) \
.addAnalyzer(Size()) \
.addAnalyzer(Completeness("CONTRACT_TYPE")) \
.run()
analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.show()

#print(json.dumps(suggestionResult, indent=2))

check = Check(spark, CheckLevel.Warning, "Amazon Electronics Review Check")
checkResult = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check.hasSize(lambda x: x >= 3000000) \
        .hasMin("star_rating", lambda x: x == 1.0) \
        .hasMax("star_rating", lambda x: x == 5.0)  \
        .isComplete("review_id")  \
        .isUnique("review_id")  \
        .isComplete("marketplace")  \
        .isContainedIn("marketplace", ["US", "UK", "DE", "JP", "FR"]) \
        .isNonNegative("year")) \
    .run()
    
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()
"""

#get_constraint_recommend("c:/ma/openai/upload","stg_repo.csv")