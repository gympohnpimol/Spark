
'''Text classification'''
'Creating a UDF'
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import length
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

# Returns true if the value is a nonempty vector
nonempty_udf = udf(lambda x:  
    True if (x and hasattr(x, "toArray") and x.numNonzeros())
    else False, BooleanType())
# Returns first element of the array as string
s_udf = udf(lambda x: str(x[0]) if (x and type(x) is list and len(x) > 0)
    else '', StringType())

'Array column'
# Show the rows where doc contains the item '5'
df_before.where(array_contains('doc', '5')).show()

# UDF removes items in TRIVIAL_TOKENS from array
rm_trivial_udf = udf(lambda x:
                     list(set(x) - TRIVIAL_TOKENS) if x
                     else x,
                     ArrayType(StringType()))

# Remove trivial tokens from 'in' and 'out' columns of df2
df_after = df_before.withColumn('in', rm_trivial_udf('in'))\
                    .withColumn('out', rm_trivial_udf('out'))

# Show the rows of df_after where doc contains the item '5'
df_after.where(array_contains('doc','5')).show()

'''Creating feature data for classification'''
'Creating a UDF for vector data'
# Selects the first element of a vector column
first_udf = udf(lambda x:
            float(x.indices[0]) 
            if (x and hasattr(x, "toArray") and x.numNonzeros())
            else 0.0,
            FloatType())

# Apply first_udf to the output column
df.select(first_udf("output").alias("result")).show(5)
'''
+------+
|result|
+------+
|  65.0|
|   8.0|
|  47.0|
|  89.0|
|  94.0|
+------+ '''

'Applying a UDF to vector data'
# Add label by applying the get_first_udf to output column
df_new = df.withColumn('label', get_first_udf('output'))
# Show the first five rows 
df_new.show(5)
'''
    +------------------+-----+
    |            output|label|
    +------------------+-----+
    |(12847,[65],[1.0])|   65|
    | (12847,[8],[1.0])|    8|
    |(12847,[47],[1.0])|   47|
    |(12847,[89],[1.0])|   89|
    |(12847,[94],[1.0])|   94|
    +------------------+-----+
'''
'Transforming text to vector format'
# Transform df using model
result = model.transform(df.withColumnRenamed('in', 'words'))\
        .withColumnRenamed('words', 'in')\
        .withColumnRenamed('vec', 'invec')
result.drop('sentence').show(3, False)

# Add a column based on the out column called outvec
result = model.transform(result.withColumnRenamed('out', 'words'))\
        .withColumnRenamed('words', 'out')\
        .withColumnRenamed('vec', 'outvec')
result.select('invec', 'outvec').show(3, False)	

'''
    +----------------------+-------+------------------------------------+
    |in                    |out    |invec                               |
    +----------------------+-------+------------------------------------+
    |[then, how, many, are]|[there]|(126,[3,18,28,30],[1.0,1.0,1.0,1.0])|
    |[how]                 |[many] |(126,[28],[1.0])                    |
    |[i, donot]            |[know] |(126,[15,78],[1.0,1.0])             |
    +----------------------+-------+------------------------------------+
    only showing top 3 rows
    
    +------------------------------------+----------------+
    |invec                               |outvec          |
    +------------------------------------+----------------+
    |(126,[3,18,28,30],[1.0,1.0,1.0,1.0])|(126,[11],[1.0])|
    |(126,[28],[1.0])                    |(126,[18],[1.0])|
    |(126,[15,78],[1.0,1.0])             |(126,[21],[1.0])|
    +------------------------------------+----------------+
'''
'Label the data'
# Import the lit function
from pyspark.sql.functions import lit

# Select the rows where endword is 'him' and label 1
df_pos = df.where("endword = 'him'")\
           .withColumn('label', lit(1))

# Select the rows where endword is not 'him' and label 0
df_neg = df.where("endword <> 'him'")\
           .withColumn('label', lit(0))

# Union pos and neg in equal number
df_examples = df_pos.union(df_neg.limit(df_pos.count()))
print("Number of examples: ", df_examples.count())
df_examples.where("endword <> 'him'").sample(False, .1, 42).show(5)

'''
+-------+--------------------+--------------------+------------------+-----+
|endword|                 doc|            features|            outvec|label|
+-------+--------------------+--------------------+------------------+-----+
|      i|[the, adventure, ...|(12847,[0,3,3766,...|(12847,[11],[1.0])|    0|
|     he|[it, is, simplici...|(12847,[7,16,23,4...| (12847,[6],[1.0])|    0|
|     it|[why, should, i, ...|(12847,[2,11,109,...| (12847,[7],[1.0])|    0|
|     it|[and, she, will, ...|(12847,[1,26,47,6...| (12847,[7],[1.0])|    0|
|   said|[there, are, thre...|(12847,[1,5,6,38,...|(12847,[23],[1.0])|    0|
+-------+--------------------+--------------------+------------------+-----+
'''

'Split the data'
# Split the examples into train and test, use 80/20 split
df_trainset, df_testset = df_examples.randomSplit((0.80, 0.20), 42)

# Print the number of training examples
print("Number training: ", df_trainset.count())

# Print the number of test examples
print("Number test: ", df_testset.count())
'''
Number training:  2091
Number test:  495
'''

'Train the classifier'
# Import the logistic regression classifier
from pyspark.ml.classification import LogisticRegression

# Instantiate logistic setting elasticnet to 0.0
logistic = LogisticRegression(maxIter=100, regParam=0.4, elasticNetParam=0.0)

# Train the logistic classifer on the trainset
df_fitted = logistic.fit(df_trainset)

# Print the number of training iterations
print("Training iterations: ", df_fitted.summary.totalIterations)
'''Training iterations:  21'''

'Evaluate the classifier'
# Score the model on test data
testSummary = df_fitted.evaluate(df_testset)

# Print the AUC metric
print("\ntest AUC: %.3f" % testSummary.areaUnderROC)
'''test AUC: 0.890'''

'Predict test data'
# Apply the model to the test data
predictions = df_fitted.transform(df_testset).select(fields)

# Print incorrect if prediction does not match label
for x in predictions.take(8):
    print()
    if x.label != int(x.prediction):
        print("INCORRECT ==> ")
    for y in fields:
        print(y,":", x[y])
'''
prediction : 1.0
label : 1
endword : him
doc : ['and', 'pierre', 'felt', 'that', 'their', 'opinion', 'placed', 'responsibilities', 'upon', 'him']
probability : [0.28537355252312796,0.714626447476872]

prediction : 1.0
label : 1
endword : him
doc : ['at', 'the', 'time', 'of', 'that', 'meeting', 'it', 'had', 'not', 'produced', 'an', 'effect', 'upon', 'him']
probability : [0.4223142404987621,0.5776857595012379]

INCORRECT ==> 
prediction : 0.0
label : 1
endword : him
doc : ['bind', 'him', 'bind', 'him']
probability : [0.5623411025382637,0.43765889746173625]

prediction : 1.0
label : 1
endword : him
doc : ['bolkonski', 'made', 'room', 'for', 'him', 'on', 'the', 'bench', 'and', 'the', 'lieutenant', 'colonel', 'sat', 'down', 'beside', 'him']
probability : [0.3683499060175795,0.6316500939824204]
'''
