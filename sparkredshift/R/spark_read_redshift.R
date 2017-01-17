
#' SQL Query from Amazon Redshift into a Spark DataFrame
#'
#'
#' @param sc a spark connection
#' @param name the name to assign to the newly generated table
#' @param redshift_url the url to the redshift database
#' @param redshift_user Amazon Redshift Username
#' @param redshift_password Amazon Redshift Password
#' @param s3bucket AWS S3 bucket address (must have write access)
#' @param s3accesskey AWS Access Key ID
#' @param s3secret AWS Access Secret
#' @param query SQL query to execute on database
#' @export

spark_read_redshift <- function(sc,
                                name,
                                query,
                                redshift_url,
                                redshift_user,
                                redshift_password,
                                s3bucket,
                                s3accesskey,
                                s3secret
) {
  if (!require(sparklyr)) !require(sparklyr)
  # get the java spark context
  jsc <- invoke_static(
    sc,
    "org.apache.spark.api.java.JavaSparkContext",
    "fromSparkContext",
    spark_context(sc)
  )

  # set hadoop config
  hadoop_config <- jsc %>% invoke('hadoopConfiguration')
  hadoop_config %>% invoke('set','fs.s3n.awsAccessKeyId',s3accesskey)
  hadoop_config %>% invoke('set','fs.s3n.awsSecretAccessKey',s3secret)


  tbl <- hive_context(sc) %>%
    invoke("read") %>%
    invoke("format", "com.databricks.spark.redshift") %>%
    invoke("option",'url',sprintf('%s?user=%s&password=%s',redshift_url,redshift_user,redshift_password)) %>%
    invoke('option','query',query) %>%
    invoke('option','tempdir',s3bucket) %>%
    invoke('load') %>% sdf_register(.,name=name)

  return(tbl)
}

spark_dependencies <- function(spark_version, scala_version, ...) {
  spark_dependency(
    packages = c(
      sprintf("com.databricks:spark-redshift_%s:0.6.0", scala_version),
      sprintf("com.databricks:spark-avro_%s:2.0.1", scala_version)
    )
  )
}
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
