"""
Etape 2 - analyses et optimisation (Silver -> Gold)

Je fais 3 analyses en SQL sur la couche Silver 
-  la gravite selon la meteo,
- les accidents par type de route (avec une jointure carac+lieux)
- le top 5 des departements par mois (avec une window function). 
A la fin je mesure aussi l'effet d'un broadcast join, avant/apres, avec les temps et les plans.
"""

# IMPORT & CHEMINS

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"


# les script sont dans src/, (output/) est un cran au dessus
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(BASE_DIR, "output", "silver")
GOLD_DIR   = os.path.join(BASE_DIR, "output", "gold")

# -------------------------------------------

# meme config que le script d'ingestion, on peux faire un copier coller (c'est justifier)
spark = (
    SparkSession.builder
    .appName("ONISR-Analyses")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.warehouse.dir", os.path.join(BASE_DIR, "spark-warehouse"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  ONISR Pipeline Etape 2 : Analyses")
print("  Spark UI : http://localhost:4040")
print("=" * 60)

# je relis direct la couche silver (le parquet deja nettoyéé par le script 1),
# pas les CSV bruts, sinon je refais tout le nettoyage pour rien
df_carac = spark.read.parquet(os.path.join(SILVER_DIR, "caracteristiques"))
df_lieux = spark.read.parquet(os.path.join(SILVER_DIR, "lieux"))
df_usag  = spark.read.parquet(os.path.join(SILVER_DIR, "usagers"))

# j'enregistre les DF en vue SQL pour pouvoir faire du spark.sql() direct dessous
df_carac.createOrReplaceTempView("carac")
df_lieux.createOrReplaceTempView("lieux")
df_usag.createOrReplaceTempView("usagers")

print(f"\nSilver charge : {df_carac.count():,} accidents, {df_usag.count():,} usagers")

# j'ai fait mes 3 analyses en SQL plutot qu'en DataFrame. Meme perf au final
# (Spark traite les deux pareil), mais je suis plus a l'aise en SQL pour ecrire des jointures et des group by
# pour tout ce qui est requetes a BDD je prefere SQL


## QUESTION 1

print("\nANALYSE 1 : Gravite des accidents par conditions meteo")

# le mauvais temps augmente vraiment le taux de mort ? 
df_a1 = spark.sql("""
    SELECT
        CASE c.atm
            WHEN 1 THEN 'Normal'
            WHEN 2 THEN 'Pluie legere'
            WHEN 3 THEN 'Pluie forte'
            WHEN 4 THEN 'Neige/grele'
            WHEN 5 THEN 'Brouillard'
            WHEN 6 THEN 'Vent fort'
            WHEN 7 THEN 'Eblouissement'
            WHEN 8 THEN 'Couvert'
            WHEN 9 THEN 'Autre'
            ELSE 'Inconnu'
        END AS meteo,
        COUNT(*)                                          AS nb_usagers,
        SUM(CASE WHEN u.grav = 2 THEN 1 ELSE 0 END)      AS nb_tues,
        SUM(CASE WHEN u.grav = 3 THEN 1 ELSE 0 END)      AS nb_hospitalises,
        ROUND(
            SUM(CASE WHEN u.grav = 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2)                                                AS taux_tues_pct
    FROM carac c
    INNER JOIN usagers u ON c.Num_Acc = u.Num_Acc
    WHERE c.atm IS NOT NULL
      AND u.grav IS NOT NULL
    GROUP BY c.atm
    ORDER BY taux_tues_pct DESC
""")
    # jointure => carac+usagers sur Num_Acc

df_a1.show(10, truncate=False)

# coalesce(1) pour sortir un seul fichier CSV lisible plutot que plein de petits
# morceaux repartis sur les differents coeurs
df_a1.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(os.path.join(GOLD_DIR, "A1_gravite_meteo"))
print("  -> Sauvegarde Gold : output/gold/A1_gravite_meteo/")


## QUESTION 2

print("\nANALYSE 2 : Accidents par type de route et milieu")

# quels types de routes sont les plus accidentogenes en ville ou hors agglo ?
df_a2 = spark.sql("""
    SELECT
        CASE l.catr
            WHEN 1 THEN 'Autoroute'
            WHEN 2 THEN 'Route nationale'
            WHEN 3 THEN 'Route departementale'
            WHEN 4 THEN 'Voie communale'
            WHEN 5 THEN 'Hors reseau public'
            WHEN 6 THEN 'Parc de stationnement'
            ELSE 'Autre'
        END AS type_route,
        CASE c.agg
            WHEN 2 THEN 'Agglomeration'
            ELSE 'Hors agglomeration'
        END AS milieu,
        COUNT(c.Num_Acc) AS nb_accidents
    FROM carac c
    INNER JOIN lieux l ON c.Num_Acc = l.Num_Acc
    WHERE l.catr IS NOT NULL
      AND c.agg IS NOT NULL
    GROUP BY l.catr, c.agg
    ORDER BY nb_accidents DESC
""")

df_a2.show(20, truncate=False)

df_a2.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(os.path.join(GOLD_DIR, "A2_accidents_par_route"))
print("  -> Sauvegarde Gold : output/gold/A2_accidents_par_route/")


## QUESTION 1

print("\nANALYSE 3 : Top 5 departements accidentogenes par mois")

# est ce que ce sont toujours les memes departements, ou ca change d'un mois a l'autre ?
# pas de jointure ici, DENSE_RANK classe les deps a l'interieur de chaque mois
df_a3 = spark.sql("""
    SELECT mois, dep, nb_accidents, rang
    FROM (
        SELECT
            mois,
            dep,
            COUNT(Num_Acc) AS nb_accidents,
            DENSE_RANK() OVER (PARTITION BY mois ORDER BY COUNT(Num_Acc) DESC) AS rang
        FROM carac
        WHERE mois IS NOT NULL
          AND dep  IS NOT NULL
        GROUP BY mois, dep
    )
    WHERE rang <= 5
    ORDER BY mois, rang
""")

df_a3.show(60, truncate=False)

df_a3.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(os.path.join(GOLD_DIR, "A3_top_dep_par_mois"))
print("  -> Sauvegarde Gold : output/gold/A3_top_dep_par_mois/")

print("\nOPTIMISATION : Broadcast Join, mesure avant/apres")

# par defaut Spark fait un SortMergeJoin (shuffle des deux tables pour aligner les Num_Acc du meme cote).
# --> broadcast() envoie df_lieux entiere sur chaque coeur (plus besoin de shuffle)
# Je coupe le broadcast auto pour que la mesure "avant" soit propre
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.enabled", "false")


df_carac.cache(); df_carac.count()
df_lieux.cache(); df_lieux.count()
    # La je cache les deux tables pour que le chrono mesure vraiment le join et pas la relecture parquet

debut = time.time()
n_normal = df_carac.join(df_lieux, "Num_Acc").count()
fin = time.time()
temps_normal = fin - debut
print(f"\n  JOIN normal    (SortMergeJoin)   : {n_normal:,} lignes en {temps_normal:.3f}s")

debut = time.time()
n_broadcast = df_carac.join(broadcast(df_lieux), "Num_Acc").count()
fin = time.time()
temps_broadcast = fin - debut
print(f"  JOIN broadcast (BroadcastHashJoin): {n_broadcast:,} lignes en {temps_broadcast:.3f}s")

gain = (temps_normal - temps_broadcast) / max(temps_normal, 0.001) * 100
print(f"  Gain : {gain:.1f}% plus rapide avec broadcast")

print("\n  -- Plan SANS broadcast (chercher SortMergeJoin) --")
df_carac.join(df_lieux, "Num_Acc").explain()

print("\n  -- Plan AVEC broadcast (chercher BroadcastHashJoin) --")
df_carac.join(broadcast(df_lieux), "Num_Acc").explain()

# je remets les configs Spark par defaut pour la suite, sinon je risque de fausser autre chose
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
spark.conf.set("spark.sql.adaptive.enabled", "true")
df_carac.unpersist()
df_lieux.unpersist()

print("\n" + "=" * 60)
print("  Etape 2 terminee. Resultats dans : output/gold/")
print("  -> Ouvrir http://localhost:4040 MAINTENANT")
print("  -> Onglet Jobs : regarder le job avec le plus de stages (A1 ou A2)")
print("=" * 60)
input("\n  Entree quand t'as fini de regarder la Spark UI...")
spark.stop()
