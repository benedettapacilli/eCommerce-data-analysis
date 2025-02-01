package utils

object Config {

  // The local directory containing this repository
  val projectDir :String = "path_to/eCommerce-behavior-data-analysis/"
  // The name of the bucket on AWS S3 containing data
  val s3bucketName :String = "<bucket_name>"
  // The path to the credentials file for AWS (should be created in /src/main/resources/aws_credentials.txt)
  val credentialsPath :String = "/aws_credentials.txt"

}