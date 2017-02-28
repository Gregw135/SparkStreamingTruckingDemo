package main.scala
import models.{EnrichedTruckAndTrafficData, WindowedDriverStats}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.streaming.dstream.DStream

/**
  * Predict violations
  */
class Predictor(modelWeights: org.apache.spark.mllib.linalg.Vector, numModelWeights: Integer) {

  val model = new StreamingLinearRegressionWithSGD().setStepSize(0.1).setMiniBatchFraction(1.0).setNumIterations(3)

  if (modelWeights != null) {
    model.setInitialWeights(modelWeights)
    println("Loaded pretrained logistic regression model")
  }else{
    model.setInitialWeights(Vectors.zeros(numModelWeights))
  }

    def predict(driverStats: DStream[EnrichedTruckAndTrafficData]): DStream[Double]={
      val vectors = driverStats.map(
        data =>
          Vectors.dense(data.routeId, data.latitude, data.longitude, data.speed, data.foggy, data.rainy, data.windy)
      )
      model.predictOn(vectors)

      //model.trainOn(driverStats)
    }

}


