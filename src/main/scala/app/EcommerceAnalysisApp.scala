package app

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.PrintWriter
import java.util.Calendar
import utils._

object EcommerceAnalysisApp {
  val inputFileOct = "/datasets/big_data_project/enriched_2019-Oct.csv"
  val inputFileNov = "/datasets/big_data_project/enriched_2019-Oct.csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Ecommerce Analysis")
      .getOrCreate()

    // Load and parse the data using the custom parser
    val rddOct = spark.sparkContext.textFile(Commons.getDatasetPath(inputFileOct))
    val rddNov = spark.sparkContext.textFile(Commons.getDatasetPath(inputFileNov))

    val parsedRDDOct = EcommerceAnalysisParser.parseRDD(rddOct)
    val parsedRDDNov = EcommerceAnalysisParser.parseRDD(rddNov)

    runOptimizedJob(spark, parsedRDDOct, parsedRDDNov)
    val jobType = if (args.nonEmpty && args(0).equalsIgnoreCase("opt")) {
      runOptimizedJob(spark, parsedRDDOct, parsedRDDNov)
    } else if (args.nonEmpty && args(0).equalsIgnoreCase("non-opt")) {
      runNonOptimizedJob(spark, parsedRDDOct, parsedRDDNov)
    }
  }

  // Non-Optimized Job
  def runNonOptimizedJob(spark: SparkSession, rddOct: RDD[(String, String, Long, Long, String, String, Double, Long, String, Int, Int, Int, String)], rddNov: RDD[(String, String, Long, Long, String, String, Double, Long, String, Int, Int, Int, String)]): Unit = {
    // --- Top Products by Total Revenue generated ---

    val totalRevenueOct = rddOct.filter(rec => rec._2 == "purchase")
      .map(rec => (rec._3, (rec._7, rec._5, rec._6)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2, a._3))

    val topRevenueOct = totalRevenueOct
      .map { case (productId, (revenue, categoryCode, brand)) => (revenue, productId, categoryCode, brand) }
      .sortBy(r => r._1, ascending = false)

    val leastRevenueOct = totalRevenueOct
      .map { case (productId, (revenue, categoryCode, brand)) => (revenue, productId, categoryCode, brand) }
      .sortBy(r => r._1, ascending = true)

    val totalRevenueNov = rddNov.filter(rec => rec._2 == "purchase")
      .map(rec => (rec._3, (rec._7, rec._5, rec._6)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2, a._3))

    val topRevenueNov = totalRevenueNov
      .map { case (productId, (revenue, categoryCode, brand)) => (revenue, productId, categoryCode, brand) }
      .sortBy(r => r._1, ascending = false)

    val leastRevenueNov = totalRevenueNov
      .map { case (productId, (revenue, categoryCode, brand)) => (revenue, productId, categoryCode, brand) }
      .sortBy(r => r._1, ascending = true)

    // --- Conversion Rates by Product ---
    val conversionRateOct = rddOct.filter(rec => rec._2 == "purchase" || rec._2 == "view")
      .map { rec =>
        val purchaseCount = if (rec._2 == "purchase") 1 else 0
        val viewCount = if (rec._2 == "view") 1 else 0
        (rec._3, (purchaseCount, viewCount, rec._5, rec._6))
      }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3, a._4))
      .mapValues { case (purchaseCount, viewCount, categoryCode, brand) =>
        val conversionRate = if (viewCount > 0) purchaseCount.toDouble / viewCount else 0.0
        (conversionRate, categoryCode, brand)
      }

    val conversionRateNov = rddNov.filter(rec => rec._2 == "purchase" || rec._2 == "view")
      .map { rec =>
        val purchaseCount = if (rec._2 == "purchase") 1 else 0
        val viewCount = if (rec._2 == "view") 1 else 0
        (rec._3, (purchaseCount, viewCount, rec._5, rec._6))
      }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3, a._4))
      .mapValues { case (purchaseCount, viewCount, categoryCode, brand) =>
        val conversionRate = if (viewCount > 0) purchaseCount.toDouble / viewCount else 0.0
        (conversionRate, categoryCode, brand)
      }

    // --- Average Visit Count per User ---
    val avgVisitCountOct = rddOct
      .map(rec => (rec._8, rec._10))
      .groupByKey()
      .mapValues(visitCounts => visitCounts.sum.toDouble / visitCounts.size)

    val avgVisitCountNov = rddNov
      .map(rec => (rec._8, rec._10))
      .groupByKey()
      .mapValues(visitCounts => visitCounts.sum.toDouble / visitCounts.size)

    // --- Conversion Rate by Age Group ---
    val ageGroupsOct = rddOct
      .map { rec =>
        val ageGroup = rec._12 match {
          case age if age >= 18 && age <= 24 => "18-24"
          case age if age >= 25 && age <= 34 => "25-34"
          case age if age >= 35 && age <= 44 => "35-44"
          case age if age >= 45 && age <= 54 => "45-54"
          case age if age >= 55 && age <= 64 => "55-64"
          case age if age >= 65 => "65+"
          case _ => "Unknown"
        }
        (ageGroup, (if (rec._2 == "purchase") 1 else 0, if (rec._2 == "view") 1 else 0))
      }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (purchaseCount, viewCount) =>
        if (viewCount > 0) purchaseCount.toDouble / viewCount else 0.0
      }

    val ageGroupsNov = rddNov
      .map { rec =>
        val ageGroup = rec._12 match {
          case age if age >= 18 && age <= 24 => "18-24"
          case age if age >= 25 && age <= 34 => "25-34"
          case age if age >= 35 && age <= 44 => "35-44"
          case age if age >= 45 && age <= 54 => "45-54"
          case age if age >= 55 && age <= 64 => "55-64"
          case age if age >= 65 => "65+"
          case _ => "Unknown"
        }
        (ageGroup, (if (rec._2 == "purchase") 1 else 0, if (rec._2 == "view") 1 else 0))
      }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues { case (purchaseCount, viewCount) =>
        if (viewCount > 0) purchaseCount.toDouble / viewCount else 0.0
      }

    // --- User Retention Rate ---
    val userViewsOct = rddOct.filter(rec => rec._2 == "view")
      .map(rec => (rec._8, 1))
      .reduceByKey(_ + _)

    val totalUsersOct = rddOct
      .map(rec => rec._8)
      .distinct()
      .count()

    val returningUsersOct = userViewsOct
      .filter { case (_, viewCount) => viewCount > 1 }
      .count()

    val retentionRateOct = if (totalUsersOct > 0) returningUsersOct.toDouble / totalUsersOct else 0.0

    val userViewsNov = rddNov.filter(rec => rec._2 == "view")
      .map(rec => (rec._8, 1))
      .reduceByKey(_ + _)

    val totalUsersNov = rddNov
      .map(rec => rec._8)
      .distinct()
      .count()

    val returningUsersNov = userViewsNov
      .filter { case (_, viewCount) => viewCount > 1 }
      .count()

    val retentionRateNov = if (totalUsersOct > 0) returningUsersNov.toDouble / totalUsersOct else 0.0
  }

  def runOptimizedJob(spark: SparkSession, rddOct: RDD[(String, String, Long, Long, String, String, Double, Long, String, Int, Int, Int, String)], rddNov: RDD[(String, String, Long, Long, String, String, Double, Long, String, Int, Int, Int, String)]): Unit = {
    val combinedRDD = rddOct.union(rddNov).cache()

    // --- Top Products by Total Revenue generated ---
    val totalRevenueCombined = combinedRDD.filter(rec => rec._2 == "purchase")
      .map(rec => (rec._3, (rec._7, rec._5, rec._6)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2, a._3))

    // --- Conversion rates (Products) ---
    val conversionRates = combinedRDD.filter(rec => rec._2 == "purchase" || rec._2 == "view")
      .map { rec =>
        val purchaseCount = if (rec._2 == "purchase") 1 else 0
        val viewCount = if (rec._2 == "view") 1 else 0
        (rec._3, (purchaseCount, viewCount, rec._5, rec._6))
      }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3, a._4))
      .mapValues { case (purchaseCount, viewCount, categoryCode, brand) =>
        val conversionRate = if (viewCount > 0) purchaseCount.toDouble / viewCount else 0.0
        (conversionRate, categoryCode, brand)
      }

    //
    val userVisitCounts = combinedRDD.filter(rec => rec._2 == "view")
                                        .map(rec => (rec._8, rec._10))
                                        .combineByKey( visitCount => (visitCount, 1),
                                          (acc: (Int, Int), visitCount: Int) => (acc._1 + visitCount, acc._2 + 1),
                                          (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
                                        )

    // --- Average Visit Count per User ---
    val avgVisitCount = userVisitCounts
      .mapValues { case (sum, count) => sum.toDouble / count }

    // --- Average Visit Count Across All Users ---
    val totalAvgVisitCount = userVisitCounts
      .mapValues { case (totalVisitCount, userCount) => totalVisitCount.toDouble / userCount }
      .map { case (_, avg) => avg }
      .mean()

    // --- Conversion Rate by Age Group  ---
    val ageGroups = combinedRDD
      .map { rec =>
        val ageGroup = rec._12 match {
          case age if age >= 18 && age <= 24 => "18-24"
          case age if age >= 25 && age <= 34 => "25-34"
          case age if age >= 35 && age <= 44 => "35-44"
          case age if age >= 45 && age <= 54 => "45-54"
          case age if age >= 55 && age <= 64 => "55-64"
          case age if age >= 65 => "65+"
          case _ => "Unknown"
        }
        (ageGroup, (if (rec._2 == "purchase") 1 else 0, if (rec._2 == "view") 1 else 0))
      }
      .reduceByKey { (a, b) =>
        (a._1 + b._1, a._2 + b._2)
      }
      .mapValues { case (purchaseCount, viewCount) =>
        if (viewCount > 0) purchaseCount.toDouble / viewCount else 0.0
      }

    // --- Conversion Rate by Gender ---
    val genderGroups = combinedRDD
      .map { rec => (rec._13, (if (rec._2 == "purchase") 1 else 0, if (rec._2 == "view") 1 else 0)) }
      .reduceByKey { (a, b) => (a._1 + b._1, a._2 + b._2) }
      .mapValues { case (purchaseCount, viewCount) =>
        if (viewCount > 0) purchaseCount.toDouble / viewCount else 0.0
      }

    // --- User Retention Rate ---
    val userViews = combinedRDD.filter(rec => rec._2 == "view")
      .map(rec => (rec._8, 1))
      .reduceByKey(_ + _)

    val totalUsers = combinedRDD.map(rec => rec._8)
      .distinct()
      .count()

    val returningUsers = userViews.filter { case (_, viewCount) => viewCount > 1 }
      .count()

    val retentionRate = if (totalUsers > 0) returningUsers.toDouble / totalUsers else 0.0
  }

}