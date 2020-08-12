
'''Model tuning and selection'''

'Create the modeler'
# Import LogisticRegression
from pyspark.ml.classification import LogisticRegression
# Create a LogisticRegression Estimator
lr = LogisticRegression()

'Create the evaluator'
# Import the evaluation submodule
import pyspark.ml.evaluation as evals
# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName="areaUnderROC")

'Make a grid to create a grid of values to search over when looking for the optimal hyperparameters'
# Import the tuning submodule
import pyspark.ml.tuning as tune
# Create the parameter grid
grid = tune.ParamGridBuilder()
# Add the hyperparameter
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0, 1])
# Build the grid
grid = grid.build()

'Make the validator'
# Create the CrossValidator
cv = tune.CrossValidator(estimator= lr,
               estimatorParamMaps=grid,
               evaluator=evaluator
               )

'Fit the model(s)'
# Call lr.fit()
best_lr = lr.fit(training)
# Print best_lr
print(best_lr)

'Evaluate the model'
# Use the model to predict the test set
test_results = best_lr.transform(test)
# Evaluate the predictions
print(evaluator.evaluate(test_results))