// Databricks notebook source
// DBTITLE 1,Conferindo dados e conexão da pasta Bronze
// MAGIC %python
// MAGIC dbutils.fs.ls("mnt/dados/bronze")

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

// DBTITLE 1,Transformando Json em Colunas
display(df.select("anuncio.*"))

// COMMAND ----------

// DBTITLE 1,Criando coluna de identificação
display(
  df.select("anuncio.*", "anuncio.endereco.*")
)

// COMMAND ----------

// DBTITLE 1,Salvando na camada Bronze
val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")
display(dados_detalhados)

// COMMAND ----------

// DBTITLE 1,Removendo Colunas
val dev_silver = dados_detalhados.drop("caracteristicas", "endereco")
display(dev_silver)

// COMMAND ----------

// DBTITLE 1,Salvando na camada Silver
val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
dev_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------


