
# Analysis of data stored in Google Cloud Platform 
# initially on Jupyter Notebook, converted to .py file
# ---------------------------------------------------

#TableID: bigquery-public-data:samples.github_nested

from google.cloud import bigquery

cl = bigquery.Client()

QUERY='''
SELECT repository.language AS lg, repository.size AS size,
    repository.watchers AS watchers, actor_attributes.location AS loc,
    repository.name AS reponame,repository.description AS description,
    actor_attributes.blog AS blog,repository.owner AS owner,
    created_at AS created
FROM `bigquery-public-data.samples.github_nested`
WHERE actor_attributes.location = 'Poland' 
    AND repository.watchers > 2000
    AND repository.language='Python'
GROUP BY lg, size, watchers, loc, reponame, description, blog, owner, created
ORDER BY created DESC
LIMIT 8
'''
qjob = cl.query(QUERY)
df = qjob.to_dataframe()
df.head()

#-----------------------------------
#TableID: bigquery-public-data.github_repos.languages

from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/home/danuta/Documents/BIGDATA/projects/MyBQProject001-key2.json"

cl = bigquery.Client()

QUERY='''
SELECT
  language.name, ROUND(max(language.bytes)/(1e+6)) AS max_MB
FROM `bigquery-public-data.github_repos.languages`, UNNEST(language) AS language
WHERE language.bytes > 446000000
GROUP BY language.name
ORDER BY max_MB ASC
LIMIT 10
'''
qjob = cl.query(QUERY)
df = qjob.to_dataframe()
df[0:10]

'''output:
	name	 max_MB
0	Python	 446.0
1	PHP	     472.0
2	q	     478.0
3	Smali	 542.0
4	Scilab	 568.0
5	Java	 734.0
6	VHDL	 734.0
7	Assembly 804.0
8	Roff	 861.0
9	C++	     897.0
'''

#-----------------------------------
# TableID: bigquery-public-data.google_analytics_sample.ga_sessions_(366)

from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/home/danuta/Documents/BIGDATA/projects/MyBQProject001-key2.json"

cl = bigquery.Client()

QUERY='''
SELECT 
DISTINCT device.operatingSystem AS os, device.browser AS browser,
    device.deviceCategory, geoNetwork.country
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
WHERE device.operatingSystem LIKE 'Linux' AND device.browser LIKE 'Safari'
ORDER BY browser ASC
LIMIT 10
'''
qjob = cl.query(QUERY)
df = qjob.to_dataframe()
df[0:9]

'''output:
	 os	   browser	deviceCategory	country
0	Linux	Safari	desktop	Armenia
1	Linux	Safari	desktop	Azerbaijan
2	Linux	Safari	desktop	United Kingdom
3	Linux	Safari	desktop	United Arab Emirates
4	Linux	Safari	desktop	Indonesia
5	Linux	Safari	desktop	India
6	Linux	Safari	desktop	Slovakia
7	Linux	Safari	desktop	Mexico
8	Linux	Safari	desktop	Czechia
'''

#------------------------------------------

# TableID: bigquery-public-data.google_analytics_sample.ga_sessions_(366)

from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/home/danuta/Documents/BIGDATA/projects/MyBQProject001-key2.json"

cl = bigquery.Client()

QUERY='''
SELECT 
DISTINCT device.operatingSystem AS os, device.browser AS browser,
     geoNetwork.country, geoNetwork.city, date 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
AND device.operatingSystem LIKE 'Macintosh' AND device.browser LIKE 'Safari'
ORDER BY browser ASC
LIMIT 10
'''
qjob = cl.query(QUERY)
df = qjob.to_dataframe()
df[0:9]

'''output:

          os	browser	  country	       city	    date
0	Macintosh	Safari	United States	Washington	20170615
1	Macintosh	Safari	Australia	not available in demo dataset	20170310
2	Macintosh	Safari	France	not available in demo dataset	20170310
3	Macintosh	Safari	United States	San Jose	20170202
4	Macintosh	Safari	United Kingdom	not available in demo dataset	20160831
5	Macintosh	Safari	Dominican Republic	not available in demo dataset	20160831
6	Macintosh	Safari	Canada	not available in demo dataset	20160807
7	Macintosh	Safari	United States	Chicago	20160807
8	Macintosh	Safari	Serbia	not available in demo dataset	20160807
'''
#-----------------------------------------

# bigquery-public-data.google_analytics_sample.ga_sessions_20170701
# sum of totals.visits on 1st July 2017

from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/home/danuta/Documents/BIGDATA/projects/MyBQProject001-key2.json"

cl = bigquery.Client()

QUERY='''
SELECT DISTINCT device.browser AS browser, device.operatingSystem AS os,
    SUM(totals.visits) AS sum
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170701`
GROUP BY browser, os
ORDER BY sum DESC
LIMIT 10
'''
qjob = cl.query(QUERY)
df = qjob.to_dataframe()
df[0:9]

'''
    browser	  os	sum
0	Chrome	Windows	448
1	Chrome	Android	417
2	Safari	iOS	391
3	Safari (in-app)	iOS	175
4	Chrome	Macintosh	145
5	Android Webview	Android	95
6	Safari	Macintosh	67
7	Firefox	Windows	59
8	Chrome	iOS	43
'''

#-----------------------------

# bigquery-public-data.google_analytics_sample.ga_sessions_20170701
# sum of totals.hits, 1st July 2017, btw 2-3 o'clock

from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/home/danuta/Documents/BIGDATA/projects/MyBQProject001-key2.json"

cl = bigquery.Client()

QUERY='''
SELECT DISTINCT device.browser AS browser, device.operatingSystem AS os,
    SUM(totals.hits) AS sum
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170701`,
    UNNEST(hits)hit
WHERE hit.hour >= 2 AND hit.hour <= 3
GROUP BY browser, os
ORDER BY sum DESC
LIMIT 10
'''
qjob = cl.query(QUERY)
df = qjob.to_dataframe()
df[0:9]

 '''
    browser	os	    sum
0	Chrome	Android	3582
1	Chrome	Windows	1226
2	Safari	iOS	597
3	Safari	Macintosh	383
4	UC Browser	Android	169
5	Chrome	Macintosh	84
6	Firefox	Windows	52
7	Internet Explorer	Windows	22
8	Chrome	Linux	8
'''
#---------------------------------

# bigquery-public-data.google_analytics_sample.ga_sessions_20170701
# top total visits in countries, 1st July 2017, 1-2 o'clock

from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ="/home/danuta/Documents/BIGDATA/projects/MyBQProject001-key2.json"

cl = bigquery.Client()

QUERY='''
SELECT DISTINCT geoNetwork.country AS country, SUM(totals.visits) AS totalVisits
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170701`,
    UNNEST(hits)hits
WHERE hits.hour >= 1 AND hits.hour <= 2
GROUP BY country
ORDER BY totalVisits DESC
LIMIT 8
'''
qjob = cl.query(QUERY)
df = qjob.to_dataframe()
df[0:8]

'''
output:
   country	totalVisits
0	India	113
1	United States	93
2	Australia	41
3	Japan	40
4	Germany	30
5	Belgium	19
6	United Kingdom	19
7	Taiwan	18
'''

-----------------------------------
