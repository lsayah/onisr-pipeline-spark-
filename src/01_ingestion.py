"""
Etape 1 - ingestion et nettoyage

Je pars des 4 CSV bruts ONISR 2022 (accidents, lieux, vehicules, usagers),
je vire les doublons et les valeurs qui n'ont pas de sens, et j'ecris tout
ca en Parquet dans une couche "Silver" partitionnee par departement.
"""

# IMPORTS & CHEMIN

import os
import time
from pyspark.sql import SparkSession


# sous Windows Spark a besoin de winutils.exe pour gerer les permissions de fichiers,
# sans ces 2 lignes ca plante direct a l'ecriture du Parquet
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

# script dans src/,(data/, output/) son un dossier au dessus
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR    = os.path.join(BASE_DIR, "data", "raw")
SILVER_DIR = os.path.join(BASE_DIR, "output", "silver")

# ---------------------

# local[*] = tous les coeurs de ma machine, 2g pour le driver ca passe large pour ce volume
spark = (
    SparkSession.builder
    .appName("ONISR-Ingestion")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.warehouse.dir", os.path.join(BASE_DIR, "spark-warehouse"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  ONISR Pipeline — Etape 1 : Ingestion")
print("  Spark UI : http://localhost:4040")
print("=" * 60)

# schemas ecrits a la main, pas d'inferSchema : sinon "01" devient l'entier 1 (le zero saute) 
# Pour les coordonnees GPS ("48,8566", virgule francaise) sont mal lues en nombre
schema_carac = StructType([
    StructField("Num_Acc", StringType(),  True),  # identifiant accident, cle de jointure
    StructField("jour",    IntegerType(), True),
    StructField("mois",    IntegerType(), True),
    StructField("an",      IntegerType(), True),
    StructField("hrmn",    StringType(),  True),  # ex "1430" = 14h30
    StructField("lum",     IntegerType(), True),  # luminosité
    StructField("dep",     StringType(),  True),  # string pour garder le "01"
    StructField("com",     StringType(),  True),
    StructField("agg",     IntegerType(), True),  # 1=hors agglo, 2=en agglo
    StructField("int",     IntegerType(), True),
    StructField("atm",     IntegerType(), True),  # meteo
    StructField("col",     IntegerType(), True),  # type de collision
    StructField("adr",     StringType(),  True),
    StructField("lat",     StringType(),  True),  # virgule fr, convertie plus tard
    StructField("long",    StringType(),  True),
])

# alignement moins soigne que le schema precedent (manuellement c'est chiant)
schema_lieux = StructType([
    StructField("Num_Acc", StringType(), True),
    StructField("catr", IntegerType(), True),  # type de route
    StructField("voie", StringType(), True),
    StructField("v1", StringType(), True),
    StructField("v2", StringType(), True),
    StructField("circ", IntegerType(), True),
    StructField("nbv", IntegerType(), True),  # nombre de voies
    StructField("vosp", IntegerType(), True),
    StructField("prof", IntegerType(), True),
    StructField("pr", StringType(), True),
    StructField("pr1", StringType(), True),
    StructField("plan", IntegerType(), True),
    StructField("lartpc", StringType(), True),
    StructField("larrout", StringType(), True),
    StructField("surf", IntegerType(), True),  # etat de la chaussée
    StructField("infra", IntegerType(), True),
    StructField("situ", IntegerType(), True),
    StructField("vma", IntegerType(), True),  # vitesse limite
])

# pareil, pas realigne, ecrit plus vite (repetitif)
schema_vehicules = StructType([
    StructField("Num_Acc", StringType(), True),
    StructField("id_vehicule", StringType(), True),
    StructField("num_veh", StringType(), True),
    StructField("senc", IntegerType(), True),
    StructField("catv", IntegerType(), True),  # type de vehicule
    StructField("obs", IntegerType(), True),
    StructField("obsm", IntegerType(), True),
    StructField("choc", IntegerType(), True),
    StructField("manv", IntegerType(), True),
    StructField("motor", IntegerType(), True),
    StructField("occutc", IntegerType(), True),
])

schema_usagers = StructType([
    StructField("Num_Acc", StringType(), True),
    StructField("id_usager", StringType(), True),  # ajoutee en 2024, pas dans les vieux fichiers
    StructField("id_vehicule", StringType(), True),
    StructField("num_veh", StringType(), True),
    StructField("place", IntegerType(), True),
    StructField("catu", IntegerType(), True),  # conducteur, passager, pieton
    StructField("grav", IntegerType(), True),  # 1=indemne 2=tue 3=hospitalise 4=blesse leger
    StructField("sexe", IntegerType(), True),
    StructField("an_nais", IntegerType(), True),
    StructField("trajet", IntegerType(), True),
    StructField("secu1", IntegerType(), True),
    StructField("secu2", IntegerType(), True),
    StructField("secu3", IntegerType(), True),
    StructField("locp", IntegerType(), True),
    StructField("actp", IntegerType(), True),
    StructField("etatp", IntegerType(), True),
])

def lire_csv(nom_fichier, schema):
    chemin = os.path.join(RAW_DIR, nom_fichier)
    return (
        spark.read
        .option("header", "true")
        .option("sep", ";")          
        .option("encoding", "latin1") # encodage des fichiers ONISR, sinon les accents font n'importe quoi
        .option("nullValue", "")      # cellules vides -> null (evite les creux)
        .schema(schema)
        .csv(chemin)
    )

df_carac = lire_csv("caract-2024.csv", schema_carac)
df_lieux = lire_csv("lieux-2024.csv", schema_lieux)
df_veh   = lire_csv("vehicules-2024.csv", schema_vehicules)
df_usag  = lire_csv("usagers-2024.csv", schema_usagers)

# petit coup d'oeil rapide avant de toucher a quoi que ce soit, histoire de voir
# si le schema est bien passe et si les valeurs ont l'air coherentes
print("\nSchéma de la table caracteristiques :")
df_carac.printSchema()

print("\nAperçu des 5 premières lignes :")
df_carac.show(5, truncate=True)

print("\nStatistiques sur les colonnes numériques :")
df_carac.describe(["jour", "mois", "an", "lum", "agg", "atm", "col"]).show()

# Je fait un count avant nettoyage pour voir combien de lignes je vais perdre au final
nb_carac_brut = df_carac.count()
nb_lieux_brut = df_lieux.count()
nb_veh_brut   = df_veh.count()
nb_usag_brut  = df_usag.count()

print(f"\nLignes brutes :")
print(f"  caracteristiques : {nb_carac_brut}")
print(f"  lieux            : {nb_lieux_brut}")
print(f"  vehicules        : {nb_veh_brut}")
print(f"  usagers          : {nb_usag_brut}")


## LE NETTOYAGE

# Doublon 
df_carac_clean = df_carac.dropDuplicates(["Num_Acc"])
    # Num_Acc est l'id unique d'un accident, donc le dedoublonnage ce fait direct dessus (simple)

# gestion num dep
df_carac_clean = df_carac_clean.withColumn(
    "dep",
    F.when(F.length(F.col("dep")) == 1, F.lpad(F.col("dep"), 2, "0"))
     .otherwise(F.col("dep"))
)
    # "1" -> "01" pour les départements a un chiffre, "971" reste intact

# gestion format heure
df_carac_clean = df_carac_clean.withColumn(
    "heure",
    F.when(
        F.col("hrmn").rlike("^\\d{1,4}$"),
        F.lpad(F.col("hrmn"), 4, "0").substr(1, 2).cast(IntegerType())
    ).when(
        F.col("hrmn").rlike("^\\d{1,2}:\\d{2}$"),
        F.split(F.col("hrmn"), ":")[0].cast(IntegerType())
    ).otherwise(None)
)
    # hrmn est stocke soit "1430" soit "14:30" selon les années du fichier, (on sait pas pourquoi)
    # du coup j'extrais juste l'heure et je gere les deux formats


# Gestion code meteo (valeur aberrantes ou erronée) 
    # --> nettoyage logique metier
df_carac_clean = df_carac_clean.withColumn(
    "atm",
    F.when(F.col("atm").between(1, 9), F.col("atm")).otherwise(None)
)
    # code météo valide entre 1 et 9, tout le reste c'est une erreur de saisie du formulaire


df_usag_clean = df_usag.dropDuplicates()
    # df_carac_clean.filter(F.col("dep") == "75").count()  # test pour verifier Paris rapidement (on laisse ? on vire ?)


# gravite valide : 1=indemne, 2=tue, 3=hospitalise, 4=blesse leger
  # --> nettoyage logique metier
df_usag_clean = df_usag_clean.withColumn(
    "grav",
    F.when(F.col("grav").between(1, 4), F.col("grav")).otherwise(None)
)


# gestion années de naissances (valeur abberantes ou erreur)
df_usag_clean = df_usag_clean.withColumn(
    "age",
    F.when(F.col("an_nais").between(1920, 2010), 2024 - F.col("an_nais"))
     .otherwise(None)
)
    # annee de naissance hors [1920-2010] = tres probablement une erreur de saisie  

# meme logique que carac et usagers juste au dessus, je vire les doublons
df_lieux_clean = df_lieux.dropDuplicates(["Num_Acc"])
df_veh_clean   = df_veh.dropDuplicates()

n_carac_clean = df_carac_clean.count()
n_usag_clean  = df_usag_clean.count()

print(f"\n Tout est clean :")
print(f"  caracteristiques : {nb_carac_brut:,} -> {nb_carac_clean:,} ({nb_carac_brut - nb_carac_clean} lignes écartées)")
print(f"  usagers          : {nb_usag_brut:,} -> {nb_usag_clean:,} ({nb_usag_brut - nb_usag_clean} lignes écartées)")


## LE PARTITIONNEMENT

# partitionne par dep --> un dossier par departement (output/silver/caracteristiques/dep=75/)
# Sert pour le partition pruning --> mesure dans 03_exploration.py
print("\n Ecriture couche Silver en Parquet")
debut_ecriture = time.time()

df_carac_clean.write \
    .mode("overwrite") \
    .partitionBy("dep") \
    .parquet(os.path.join(SILVER_DIR, "caracteristiques"))

# les autres tables restent en un seul bloc, pas besoin de les partitionner elles
df_lieux_clean.write \
    .mode("overwrite") \
    .parquet(os.path.join(SILVER_DIR, "lieux"))

df_usag_clean.write \
    .mode("overwrite") \
    .parquet(os.path.join(SILVER_DIR, "usagers"))

df_veh_clean.write \
    .mode("overwrite") \
    .parquet(os.path.join(SILVER_DIR, "vehicules"))

fin_ecriture = time.time()
print(f"  Silver ecrit en {fin_ecriture - debut_ecriture:.1f}s")  # note a moi même --> oublie pas de mettre les chiffres dans le rapport
print(f"  Localisation : {SILVER_DIR}/")
print(f"  caracteristiques/ est partitionnee par dep= (un dossier par departement)")

print("\n" + "=" * 60)
print("  Etape 1 terminee.")
print("  -> Ouvrir http://localhost:4040 MAINTENANT pour capturer la Spark UI")
print("  -> DAG visible dans l'onglet 'Jobs' -> dernier job")
print("=" * 60)
input("\n  Entree pour fermer la session Spark, sinon elle reste ouverte pour rien...")
spark.stop()
