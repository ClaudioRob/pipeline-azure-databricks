// Databricks notebook source
// DBTITLE 1,Conferindo dados e conexão da pasta Inbound
// MAGIC %python
// MAGIC dbutils.fs.ls("mnt/dados/inbound")

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// DBTITLE 1,Removendo Colunas
val dados_anuncio = dados.drop("imagens", "usuario")
display(dados_anuncio)

// COMMAND ----------

// DBTITLE 1,Criando coluna de identificação
import org.apache.spark.sql.functions.col

val df_bronze = dados_anuncio.withColumn("Id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// DBTITLE 1,Salvando na camada Bronze
val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


