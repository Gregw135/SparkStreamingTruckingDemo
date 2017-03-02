package main.scala

import com.orendainx.hortonworks.trucking.common.models.TruckEventTypes
import models.{EnrichedTruckAndTrafficData, WindowedDriverStats}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
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

  /**
    * Train the model
    * @param driverStats
    * @return
    */
    def train(driverStats: DStream[EnrichedTruckAndTrafficData]){
      val labeledPoints = driverStats.map(
        data => {
          val isViolation = (if (data.eventType == TruckEventTypes.Normal) 0 else 1)
          LabeledPoint(isViolation, Vectors.dense(data.routeId, data.latitude, data.longitude, data.speed, data.foggy, data.rainy, data.windy))
        })
      model.trainOn(labeledPoints)
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


