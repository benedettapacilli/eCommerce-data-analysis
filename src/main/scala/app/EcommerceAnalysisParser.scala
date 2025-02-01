package app

import org.apache.spark.rdd.RDD

object EcommerceAnalysisParser {

  /** Function to parse rows of the eCommerce dataset with all columns
   *
   *  @param row line to be parsed
   *  @return tuple containing all fields, or None in case of input errors
   */
  private def parseRow(row: String): Option[(String, String, Long, Long, String, String, Double, Long, String, Int, Int, Int, String)] = {
    try {
      val columns = row.split(",")

      val event_time = columns(0).trim
      val event_type = columns(1).trim
      val product_id = columns(2).trim.toLong
      val category_id = columns(3).trim.toLong
      val category_code = columns(4).trim
      val brand = columns(5).trim
      val price = columns(6).trim.toDouble
      val user_id = columns(7).trim.toLong
      val user_session_id = columns(8).trim
      val session_duration = columns(9).trim.toInt
      val visit_count = columns(10).trim.toInt
      val age = columns(11).trim.toInt
      val gender = columns(12).trim

      Some((event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session_id, session_duration, visit_count, age, gender))
    } catch {
      case _: Exception =>
        println(s"Error parsing row: $row")
        None
    }
  }

  /** Parse the RDD and filter out invalid rows
   *
   *  @param rdd the RDD to be parsed
   *  @return an RDD of valid parsed data
   */
  def parseRDD(rdd: RDD[String]): RDD[(String, String, Long, Long, String, String, Double, Long, String, Int, Int, Int, String)] = {
    rdd.flatMap(row => parseRow(row))
  }
}
