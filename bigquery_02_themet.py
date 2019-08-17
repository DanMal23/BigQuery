# data from Google BigQuery resources
# 'the_met' dataset: The Metropolitan Museum of Art

# all museum's departments - in alphabetical order
from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/danuta/Documents/BIGDATA/projects/Session16sie-key.json"
cl = bigquery.Client()
QUERY='''
SELECT
    department,
    SUM(CASE WHEN is_highlight THEN 1 ELSE 0 END) AS highlight,
    SUM(CASE WHEN is_highlight THEN 0 ELSE 1 END) AS non_highlight
FROM `bigquery-public-data.the_met.objects` 
GROUP BY department
ORDER BY department ASC
'''
qjob = cl.query(QUERY)
met = qjob.to_dataframe()
met
'''
		department 					highlight 	non_highlight
0 	American Decorative Arts 			62 		8880
1 	American Paintings and Sculpture 	101 	4682
2 	Ancient Near Eastern Art 			75 		6098
3 	Arms and Armor 						52 		4200
4 	Arts of Africa, Oceania,
		and the Americas 				57 		5964
5 	Asian Art 							55 		29789
6 	Costume Institute 					37 		7787
7 	Drawings and Prints 				53 		43435
8 	Egyptian Art 						114 	12164
9 	European Paintings 					99 		2223
10 	European Sculpture 
	and Decorative Arts 				79 		30532
11 	Greek and Roman Art 				115 	12403
12 	Islamic Art 						118 	10317
13 	Medieval Art 						54 		6784
14 	Modern and Contemporary Art 		18 		1094
15 	Musical Instruments 				71 		1180
16 	Photographs 						50 		6533
17 	Robert Lehman Collection 			90 		2321
18 	The Cloisters 						58 		2210
19 	The Libraries 						120 	0
'''

#----------------------------------
#oldest artifacts from Asian Art department, highlight

from google.cloud import bigquery
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/danuta/Documents/BIGDATA/projects/Session16sie-key.json"
cl = bigquery.Client()
QUERY='''
SELECT
    object_begin_date AS begin_date, object_name AS Asian_object,
    culture, credit_line
FROM `bigquery-public-data.the_met.objects`
WHERE (is_highlight=true)
	AND culture IS NOT NULL
	AND department LIKE 'Asian Art'
GROUP BY object_begin_date,object_name, culture, credit_line
ORDER BY object_begin_date ASC
LIMIT 10
'''
qjob = cl.query(QUERY)
met = qjob.to_dataframe()
met[0:10]

'''
begin_date 	Asian_object 		culture 				credit_line
0 	-1035 	Altar set 			China 					Munsey Fund, 1931
1 	-299 	Pendant 			China 					Gift of Ernest Erickson Foundation, 1985
2 	-199 	Figure 				China 					Charlotte C. and John C. Weber Collection, Gif...
3 	-100 	Ceremonial object 	Indonesia (Sulawesi) 	Purchase, George McFadden Gift and Edith Perry...
4 	-45 	Yaksha 				India (Madhya Pradesh) 	Gift of Jeffrey B. Soref, in honor of Martin L...
5 	350 	Torso 				Pakistan 				Purchase, Lila Acheson Wallace Gift, 1995
					(ancient region of Gandhara, mondern ... 	
6 	467 	Figure 				India 					Purchase, Enid A. Haupt Gift, 1979
						(Uttar Pradesh, Mathura) 	
7 	524 	Altarpiece 			China 					Rogers Fund, 1938
8 	600 	Figure 				China 					Rogers Fund, 1919
9 	636 	Figure 				Korea 					Purchase, Walter and Leonore Annenberg and The...
'''

#-------------------------------------

