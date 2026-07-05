"""
Etape 3 - exploration : je mesure le pushdown

J'ai pris la piste "pushdown mesure" (section 5 du sujet). En gros je lis la couche Silver partitionnée par dep
je chronometre une agrégation sans filtre puis avec un filtre sur dep 
Aussi je regarde le plan d'execution (.explain()) pour verifier que Spark lit vraiment moins de fichiers 
à chaque fois. Je jette aussi un oeil à l'effet de l'AQE a la fin.

Pour lancer => python src/03_exploration.py
Spark UI : http://localhost:4040
"""

import os
import time
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
from pyspark.sql import functions as F

# le script est dans src/, la racine du projet (output/) est un cran au dessus
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(BASE_DIR, "output", "silver")

spark = (
    SparkSession.builder
    .appName("ONISR-Exploration-Pushdown")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    # je coupe l'AQE et le broadcast auto pour avoir des mesures stables, sinon
    # Spark ajuste des trucs tout seul et je compare plus vraiment la meme chose
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  ONISR Pipeline — Etape 3 : Exploration Pushdown")
print("  Spark UI : http://localhost:4040")
print("=" * 60)

print("""
Petit rappel avant de commencer : ma couche Silver est en Parquet, partitionnee
par dep. Sur le disque ca donne un dossier par departement :
    output/silver/caracteristiques/
      dep=01/  part-00000.parquet
      dep=02/  part-00000.parquet
      ...
      dep=75/  part-00000.parquet
      ...
      dep=976/ part-00000.parquet

Si je filtre sur dep="75", Spark va lire direct le dossier dep=75/ et zappe
les autres, il a meme pas besoin d'y toucher. C'est ca le partition pruning.

Et pour les colonnes qui sont dans le fichier Parquet mais pas dans le nom
du dossier (genre atm), Spark peut pousser le filtre direct dans le lecteur
Parquet, avant meme de charger les lignes en memoire. Ca, c'est le predicate
pushdown.
""")

# je relis la couche Silver partitionnee par dep, deja ecrite par le script 1
df = spark.read.parquet(os.path.join(SILVER_DIR, "caracteristiques"))

print("\nTEST 1 : Sans filtre, lit tous les departements")

spark.catalog.clearCache()  # sinon un cache d'un test precedent fausse le chrono suivant

t_debut = time.time()
n_total = df.agg(F.count("Num_Acc")).collect()[0][0]
t_fin = time.time()
temps_sans_filtre = t_fin - t_debut

print(f"  Total accidents : {n_total:,}")
print(f"  Temps           : {temps_sans_filtre:.3f}s")
print("\n  -> Dans Spark UI (port 4040) :")
print("     Onglet 'Jobs' -> dernier job -> cliquer sur le stage FileScan")
print("     Chercher 'number of files read' : devrait etre >= nb de departements")

print("\n  Plan d'execution (sans filtre) :")
df.agg(F.count("Num_Acc")).explain()
# je cherche "PartitionFilters: []" dans le plan -> aucun filtre, tout est lu, logique

print("\nTEST 2 : Avec filtre dep='75' (Paris) -> Partition Pruning")

spark.catalog.clearCache()

t_debut = time.time()
n_paris = df.filter(F.col("dep") == "75").agg(F.count("Num_Acc")).collect()[0][0]
t_fin = time.time()
temps_dep_filtre = t_fin - t_debut

print(f"  Accidents a Paris (dep=75) : {n_paris:,}")
print(f"  Temps                      : {temps_dep_filtre:.3f}s")
print(f"  Gain                       : {temps_sans_filtre / max(temps_dep_filtre, 0.001):.1f}x plus rapide")

print("\n  Plan d'execution (filtre dep='75') :")
df.filter(F.col("dep") == "75").explain()
# la je cherche "PartitionFilters: [isnotnull(dep#...), (dep#... = 75)]"
# ca prouve que Spark n'a lu QUE le fichier dep=75/, pas les autres departements

# df.filter(F.col("dep") == "13").agg(F.count("Num_Acc")).collect()  # essai sur Marseille, a virer

print("\nTEST 3 : Filtre sur atm (colonne non-partition) -> Predicate Pushdown")

spark.catalog.clearCache()

# double filtre : dep (partition) + atm (colonne parquet interne)
t_debut = time.time()
n_neige = (
    df.filter((F.col("dep") == "75") & (F.col("atm") == 4))  # neige/grele a Paris
      .agg(F.count("Num_Acc")).collect()[0][0]
)
t_fin = time.time()
temps_double_filtre = t_fin - t_debut

print(f"  Accidents sous neige/grele a Paris : {n_neige:,}")
print(f"  Temps                              : {temps_double_filtre:.3f}s")

print("\n  Plan d'execution (dep='75' + atm=4) :")
df.filter((F.col("dep") == "75") & (F.col("atm") == 4)).explain()
# "PartitionFilters: [...(dep = 75)]"                <- ca c'est le partition pruning
# "PushedFilters: [IsNotNull(atm), EqualTo(atm,4)]"   <- et ca le predicate pushdown

print("\nTEST 4 : Impact de l'AQE (Adaptive Query Execution)")

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.catalog.clearCache()

t_debut = time.time()
df.groupBy("dep").agg(F.count("Num_Acc").alias("n")).orderBy(F.desc("n")).collect()
t_fin = time.time()
temps_avec_aqe = t_fin - t_debut

spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.catalog.clearCache()

t_debut = time.time()
df.groupBy("dep").agg(F.count("Num_Acc").alias("n")).orderBy(F.desc("n")).collect()
t_fin = time.time()
temps_sans_aqe = t_fin - t_debut

print(f"  groupBy(dep).count() AVEC AQE  : {temps_avec_aqe:.3f}s")
print(f"  groupBy(dep).count() SANS AQE  : {temps_sans_aqe:.3f}s")
print(f"  Difference : {abs(temps_avec_aqe - temps_sans_aqe):.3f}s")
print("\n  Avec AQE, Spark ajuste le nombre de partitions shuffle dynamiquement.")
print("  Pour de petits volumes, l'effet est limité — mais sur grand volume,")
print("  AQE evite de creer des milliers de partitions vides apres un groupBy.")

print("\n" + "=" * 60)
print("  RECAP DES 4 TESTS")
print("=" * 60)
print(f"  Test 1 — sans filtre (tous les deps) : {temps_sans_filtre:.3f}s")
print(f"  Test 2 — dep='75' seulement          : {temps_dep_filtre:.3f}s  "
      f"({temps_sans_filtre/max(temps_dep_filtre,0.001):.1f}x plus rapide)")
print(f"  Test 3 — dep='75' + atm=4            : {temps_double_filtre:.3f}s")
print(f"  Test 4 — AQE on vs off                : {temps_avec_aqe:.3f}s vs {temps_sans_aqe:.3f}s")
print()
print("  CE QUE J'EN RETIENS :")
print("  le partition pruning c'est clairement ce qui marche le mieux ici,")
print("  Spark va lire QUE les fichiers des departements que je filtre, point.")
print("  le predicate pushdown ca aide en plus sur les colonnes qui sont pas")
print("  dans le nom du dossier, ca evite de charger des lignes pour rien.")
print("  les deux marchent tout seuls du moment que la couche Silver est")
print("  bien partitionnee et que mes filtres tapent les bonnes colonnes.")

print("\n" + "=" * 60)
print("  Etape 3 terminee.")
print("  -> Ouvrir http://localhost:4040 MAINTENANT")
print("  -> Onglet 'Jobs' -> job FileScan -> voir 'number of files read'")
print("  -> Comparer Test1 (tous fichiers) vs Test2 (1 fichier dep=75)")
print("=" * 60)
input("\n  Voila, appuyer sur Entree quand j'ai fini de capturer la Spark UI...")
spark.stop()
