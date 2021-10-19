# Databricks notebook source
from statistics import mean, stdev
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType


@pandas_udf(returnType=FloatType())
def avg_func(v):
    res = []
    for vals in v:
        res.append(mean([float(i) for i in vals]))
    return pd.Series(res)


@pandas_udf(returnType=FloatType())
def stdev_func(v):
    res = []
    for vals in v:
        res.append(stdev([float(i) for i in vals]))
    return pd.Series(res)
