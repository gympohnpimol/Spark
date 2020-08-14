
'''Using window function sql for natural language processing'''

'Loading a dataframe from a parquet file'
# Load the dataframe
df = spark.read.load('sherlock_sentences.parquet')
# Filter and show the first 5 rows
df.where('id > 70').show(5, truncate=False)
'''
    +--------------------------------------------------------+---+
    |clause                                                  |id |
    +--------------------------------------------------------+---+
    |i answered                                              |71 |
    |indeed i should have thought a little more              |72 |
    |just a trifle more i fancy watson                       |73 |
    |and in practice again i observe                         |74 |
    |you did not tell me that you intended to go into harness|75 |
    +--------------------------------------------------------+---+
    only showing top 5 rows'''

'Split and explode a text column'
# Split the clause column into a column called words 
split_df = clauses_df.select(split('clause', ' ').alias('words'))
split_df.show(5, truncate=False)
'''
+-----------------------------------------------+
|words                                          |
+-----------------------------------------------+
|[title]                                        |
|[the, adventures, of, sherlock, holmes, author]|
|[sir, arthur, conan, doyle, release, date]     |
|[march, 1999]                                  |
|[ebook, 1661]                                  |
+-----------------------------------------------+
only showing top 5 rows'''

# Explode the words column into a column called word 
exploded_df = split_df.select(explode('words').alias('word'))
exploded_df.show(10)
'''
+----------+
|      word|
+----------+
|     title|
|       the|
|adventures|
|        of|
|  sherlock|
|    holmes|
|    author|
|       sir|
|    arthur|
|     conan|
+----------+
only showing top 10 rows'''

print("\nNumber of rows: ", exploded_df.count())
'Number of rows:  1279'

'''Moving Window Analysis'''
'Creating context window feature data'
# Word for each row, previous two and subsequent two words
query = """
SELECT
part,
LAG(word, 2) OVER(PARTITION BY part ORDER BY id) AS w1,
LAG(word, 1) OVER(PARTITION BY part ORDER BY id) AS w2,
word AS w3,
LEAD(word, 1) OVER(PARTITION BY part ORDER BY id) AS w4,
LEAD(word, 2) OVER(PARTITION BY part ORDER BY id) AS w5
FROM text
"""
spark.sql(query).where("part = 12").show(10)

'''
    +----+---------+---------+---------+---------+---------+
    |part|       w1|       w2|       w3|       w4|       w5|
    +----+---------+---------+---------+---------+---------+
    |  12|     null|     null|      xii|      the|adventure|
    |  12|     null|      xii|      the|adventure|       of|
    |  12|      xii|      the|adventure|       of|      the|
    |  12|      the|adventure|       of|      the|   copper|
    |  12|adventure|       of|      the|   copper|  beeches|
    |  12|       of|      the|   copper|  beeches|       to|
    |  12|      the|   copper|  beeches|       to|      the|
    |  12|   copper|  beeches|       to|      the|      man|
    |  12|  beeches|       to|      the|      man|      who|
    |  12|       to|      the|      man|      who|    loves|
    +----+---------+---------+---------+---------+---------+
only showing top 10 rows '''

'Repartitioning the data'
# Repartition text_df into 12 partitions on 'chapter' column
repart_df = text_df.repartition(12, 'chapter')
# Prove that repart_df has 12 partitions
repart_df.rdd.getNumPartitions()

'''Common word sequences'''
'Finding common word sequences'
query = """
SELECT w1, w2, w3, w4, w5, COUNT(*) AS count FROM (
   SELECT word AS w1,
   LEAD(word, 1) OVER(PARTITION BY part ORDER BY id) AS w2,
   LEAD(word, 2) OVER(PARTITION BY part ORDER BY id) AS w3,
   LEAD(word, 3) OVER(PARTITION BY part ORDER BY id) AS w4,
   LEAD(word, 4) OVER(PARTITION BY part ORDER BY id) AS w5
   FROM text
)
GROUP BY w1, w2, w3, w4, w5
ORDER BY count DESC
LIMIT 10 """
df = spark.sql(query)
df.show()

'''
    +-----+---------+------+-------+------+-----+
    |   w1|       w2|    w3|     w4|    w5|count|
    +-----+---------+------+-------+------+-----+
    |   in|      the|  case|     of|   the|    4|
    |    i|     have|    no|  doubt|  that|    3|
    | what|       do|   you|   make|    of|    3|
    |  the|   church|    of|     st|monica|    3|
    |  the|      man|   who|entered|   was|    3|
    |dying|reference|    to|      a|   rat|    3|
    |    i|       am|afraid|   that|     i|    3|
    |    i|    think|  that|     it|    is|    3|
    |   in|      his| chair|   with|   his|    3|
    |    i|     rang|   the|   bell|   and|    3|
    +-----+---------+------+-------+------+-----+ '''

'Unique 5-tuples in sorted order'
query = """
SELECT distinct w1, w2, w3, w4, w5 FROM (
   SELECT word AS w1,
   LEAD(word,1) OVER(PARTITION BY part ORDER BY id ) AS w2,
   LEAD(word,2) OVER(PARTITION BY part ORDER BY id ) AS w3,
   LEAD(word,3) OVER(PARTITION BY part ORDER BY id ) AS w4,
   LEAD(word,4) OVER(PARTITION BY part ORDER BY id ) AS w5
   FROM text
)
ORDER BY w1 DESC, w2 DESC, w3 DESC, w4 DESC, w5 DESC
LIMIT 10
"""
df = spark.sql(query)
df.show()

'''
+----------+------+---------+------+-----+
|        w1|    w2|       w3|    w4|   w5|
+----------+------+---------+------+-----+
|   zealand| stock|   paying|     4|  1/4|
|   youwill|   see|     your|   pal|again|
|   youwill|    do|     come|  come| what|
|     youth|though|   comely|    to| look|
|     youth|    in|       an|ulster|  who|
|     youth|either|       it|     s| hard|
|     youth| asked| sherlock|holmes|  his|
|yourselves|  that|       my|  hair|   is|
|yourselves|behind|    those|  then| when|
|  yourself|  your|household|   and|  the|
+----------+------+---------+------+-----+ '''

'Most frequent 3-tuples per chapter'
#   Most frequent 3-tuple per chapter
query = """
SELECT chapter, w1, w2, w3, count FROM
(
  SELECT
  chapter,
  ROW_NUMBER() OVER (PARTITION BY chapter ORDER BY count DESC) AS row,
  w1, w2, w3, count
  FROM ( %s )
)
WHERE row = 1
ORDER BY chapter ASC
""" % subquery

'''
    +-------+-------+--------+-------+-----+
    |chapter|     w1|      w2|     w3|count|
    +-------+-------+--------+-------+-----+
    |      1|     up|      to|    the|    6|
    |      2|    one|      of|    the|    8|
    |      3|     mr|  hosmer|  angel|   13|
    |      4|   that|      he|    was|    8|
    |      5|   that|      he|    was|    6|
    |      6|neville|      st|  clair|   15|
    |      7|   that|       i|     am|    7|
    |      8|     dr|grimesby|roylott|    8|
    |      9|   that|      it|    was|    7|
    |     10|   lord|      st|  simon|   28|
    |     11|      i|   think|   that|    8|
    |     12|    the|  copper|beeches|   10|
    +-------+-------+--------+-------+-----+'''
    