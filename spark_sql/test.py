from pyspark.sql.functions import *


path = "../../BDCC/projects_20181008.csv"

data = sqlContext.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(path)

"""
Total amount of projects per country. [Yearly grades (active projects in a year)]
"""
total_projects_per_country = data.select('CUSTOMER_COUNTRY', 'PROJECT', 'PROJECT_STATE')\
    .filter('PROJECT_STATE != "On Hold"')\
    .groupBy('CUSTOMER_COUNTRY')\
    .count()\
    .orderBy('CUSTOMER_COUNTRY')\
    .toDF('CUSTOMER_COUNTRY', 'TOTAL_PROJECTS')

total_projects_per_country.toPandas()

"""
Total amount of projects for Lviv [Yearly grades]
"""
total_lviv_projects = data.select('*').filter('PROJECT == "EPM-LVIV"')
total_lviv_projects.count()

"""
Industries. Industries distribution over location (country/offices)
"""
distribution = data.select('CUSTOMER_COUNTRY', 'PROJECT_INDUSTRY')\
    .distinct()\
    .toDF('CUSTOMER_COUNTRY', 'PROJECT_INDUSTRY')\
    .orderBy('CUSTOMER_COUNTRY')
distribution.toPandas()

"""
Customer origin distribution
"""
distribution = data.select('PROJECT_INDUSTRY', 'CUSTOMER')\
    .distinct()\
    .toDF('PROJECT_INDUSTRY', 'CUSTOMER')\
    .orderBy('PROJECT_INDUSTRY')
distribution.toPandas()

"""
Average project duration (for closed projects). Per industry and per location
"""
duration_by_industry = data.select('PROJECT_INDUSTRY', 'PROJECT_DURATION_IN_YEARS')\
    .filter('PROJECT_STATE != "Closed"')\
    .groupBy('PROJECT_INDUSTRY')\
    .avg()\
    .toDF('PROJECT_INDUSTRY', 'AVG_PROJECT_DURATION_IN_YEARS')\
    .orderBy(desc('AVG_PROJECT_DURATION_IN_YEARS'))

duration_by_location = data.select('CUSTOMER_COUNTRY', 'PROJECT_DURATION_IN_YEARS')\
    .filter('PROJECT_STATE != "Closed"')\
    .groupBy('CUSTOMER_COUNTRY')\
    .avg()\
    .toDF('CUSTOMER_COUNTRY', 'AVG_PROJECT_DURATION_IN_YEARS')\
    .orderBy(desc('AVG_PROJECT_DURATION_IN_YEARS'))

"""
Average project size (for all projects) Per industry, per specification and per location
"""
project_size_by_industry = data.select('PROJECT_INDUSTRY', 'MAX_EMPLOYEES_ON_PROJECT')\
    .groupBy('PROJECT_INDUSTRY')\
    .avg()\
    .toDF('PROJECT_INDUSTRY', 'AVG_MAX_EMPLOYEES_ON_PROJECT')\
    .orderBy(desc('AVG_MAX_EMPLOYEES_ON_PROJECT'))

project_size_by_specification = data.select('PRIMARY_COMPETENCY_NAME', 'MAX_EMPLOYEES_ON_PROJECT')\
    .groupBy('PRIMARY_COMPETENCY_NAME')\
    .avg()\
    .toDF('PRIMARY_COMPETENCY_NAME', 'AVG_MAX_EMPLOYEES_ON_PROJECT')\
    .orderBy(desc('AVG_MAX_EMPLOYEES_ON_PROJECT'))

project_size_by_location = data.select('CUSTOMER_COUNTRY', 'MAX_EMPLOYEES_ON_PROJECT')\
    .groupBy('CUSTOMER_COUNTRY')\
    .avg()\
    .toDF('CUSTOMER_COUNTRY', 'AVG_MAX_EMPLOYEES_ON_PROJECT')\
    .orderBy(desc('AVG_MAX_EMPLOYEES_ON_PROJECT'))

"""
projects by industry
"""
projects = data.select('PROJECT_INDUSTRY', 'PROJECT')\
    .groupBy('PROJECT_INDUSTRY')\
    .count()\
    .orderBy('PROJECT_INDUSTRY')\
    .toDF('PROJECT_INDUSTRY', 'TOTAL_PROJECTS')\
    .orderBy(desc('TOTAL_PROJECTS'))
