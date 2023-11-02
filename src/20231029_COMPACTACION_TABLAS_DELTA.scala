// --------------------------------------------------
//  ## 1. COMPACTACIÓN AUTOMATICA DE TABLAS DELTA
// --------------------------------------------------

// COMANDO OPCIONAL PARA LIMPIAR EL DIRECTORIO
//  %fs rm -r dbfs:///user/hive/warehouse/nonoptimal_covid_nyt_manual

// 1.0 REVISAMOS LA CANTIDAD DE ARCHIVOS PARQUET CON LOS QUE TRABAJEMOS
//  %fs ls dbfs:///FileStore/_spark/data/COVID-19_NYT

// 1.1. LECTURA DE ARCHIVOS PARQUET
val df = spark.read.format("parquet").load("dbfs:///FileStore/_spark/data/COVID-19_NYT")


// 1.1.1. CONTAMOS LA CANTIDAD DE REGISTROS DE LA LECTURA ANTERIOR
df.count()


// 1.2. ESCRIBIMOS UNA TABLA NO OPTIMIZADA
df.repartition(9000)
  .write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("default.nonoptimal_covid_nyt")


// 1.3. REVISAMOS LOS DETALLES DE LA TABLA
spark.sql("DESCRIBE DETAIL default.nonoptimal_covid_nyt").select("name","location","numFiles").show(truncate=false)


// 1.3.1 REVISAMOS EL SISTEMA DE ARCHIVOS
//  %fs ls dbfs:///user/hive/warehouse/nonoptimal_covid_nyt


// 1.4. EJECUTAMOS LA COMPACTACIÓN AUTOMATICA DE LA TABLA
spark.sql("OPTIMIZE default.nonoptimal_covid_nyt")


// 1.5. VOLVEMOS A REVISAR LOS DETALLES DE LA TABLA
spark.sql("DESCRIBE DETAIL default.nonoptimal_covid_nyt").select("name","location","numFiles").show(truncate=false)



// --------------------------------------------------
//  ## 2. COMPACTACIÓN MANUAL DE TABLAS DELTA
// --------------------------------------------------


// COMANDO OPCIONAL PARA LIMPIAR EL DIRECTORIO
// %fs rm -r dbfs:///user/hive/warehouse/nonoptimal_covid_nyt_manual


// 2.1. LECTURA DE ARCHIVOS PARQUET
val df = spark.read.format("parquet").load("dbfs:///FileStore/_spark/data/COVID-19_NYT")


// 2.2. ESCRIBIMOS UNA TABLA NO OPTIMIZADA
df.repartition(5000)
  .write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("default.nonoptimal_covid_nyt_manual")


// 2.3. VERIFICAMOS LA CANTIDAD DE ARCHIVOS SEGÚN LA METADATA
spark.sql("DESCRIBE DETAIL default.nonoptimal_covid_nyt_manual").select("name","location","numFiles").show(truncate=false)



// 2.4. SOBREESCRIBIMOS LA TABLA CON LA CANTIDAD DE PARTICIONES QUE NECESITAMOS
import io.delta.tables._

val deltaNonOptimal = DeltaTable.forName(spark, "default.nonoptimal_covid_nyt_manual")

val dfNonOptimal = deltaNonOptimal.toDF

dfNonOptimal
  .coalesce(1)
  .write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("default.nonoptimal_covid_nyt_manual")


// 2.3. VERIFICAMOS LA CANTIDAD DE ARCHIVOS SEGÚN LA METADATA
spark.sql("DESCRIBE DETAIL default.nonoptimal_covid_nyt_manual").select("name","location","numFiles").show(truncate=false)


// 2.3. VERIFICAMOS LA CANTIDAD DE ARCHIVOS FISICOS EN EL SISTEMA DE ARCHIVOS
// %fs ls dbfs:///user/hive/warehouse/nonoptimal_covid_nyt_manual


// --------------------------------------------------
//  ## 3. LIMPIEZA DE ARCHIVOS OBSOLETOS
// --------------------------------------------------

// 3.1. EL COMANDO VACUUM ELIMINA LOS ARCHIVOS OBSOLETOS QUE DEJA UNA TABLA DELTA
spark.sql("VACUUM default.nonoptimal_covid_nyt_manual RETAIN 0 HOURS")


// 3.2. POR LO GENERAL DELTA EVITA LA LIMPIEZA ACCIDENTAL DE ARCHIVOS QUE SEA MENOR A 7 DÍAS (168 HORAS)
// Deshabilitar la comprobación de retención de duración  (No recomendado para entornos de producción)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark.sql("VACUUM default.nonoptimal_covid_nyt_manual RETAIN 0 HOURS")


// 3.3. VERIFICAMOS LA CANTIDAD DE ARCHIVOS FISICOS EN EL SISTEMA DE ARCHIVOS
// %fs ls dbfs:///user/hive/warehouse/nonoptimal_covid_nyt_manual


// 3.4. VERIFICAMOS LA CANTIDAD DE ARCHIVOS SEGÚN LA METADATA
spark.sql("DESCRIBE DETAIL default.nonoptimal_covid_nyt_manual").select("name","location","numFiles").show(truncate=false)
