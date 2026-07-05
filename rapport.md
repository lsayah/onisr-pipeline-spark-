# Rapport - Pipeline Spark ONISR 2024

**Etudiant** : AKOUDAD Elias
**Date** : 26 juin 2026
**Données** : Accidents corporels de la circulation, ONISR 2024

---

## 1. Les Données

Les données viennent de data.gouv.fr, faites par l'ONISR (Observatoire National Interministeriel de la Securite Routiere). On à tous les accidents corporels declares en France sur l'annee 2024, decoupés en 4 fichiers CSV reliés entre eux par un identifiant commun (la clé de jointure) :      --> `Num_Acc`.

4 fichiers, tous join entre eucx par `Num_Acc` :
- **caract-2024.csv** (54 402 lignes) : un accident par ligne, avec la date, le lieu, la meteo...
- **lieux-2024.csv** (70 248 lignes) : le contexte routier de chaque accident
- **vehicules-2024.csv** (92 678 lignes) : les vehicules impliques
- **usagers-2024.csv** (125 187 lignes) : les personnes impliquees (gravite, age, sexe...)

Il il y a plus de lignes dans usagers que dans caracteristiques, c'est normal --> un seul accident peut logiquement impliquer plusieurs personnes.

Les colonnes clés que j'ai utilisees :
- `Num_Acc` (dans toutes les tables) : identifiant de l'accident, cle de jointure
- `atm` (caracteristiques) : conditions meteo (1=normal, 5=brouillard...)
- `dep` (caracteristiques) : departement
- `mois` (caracteristiques) : mois de l'accident
- `agg` (caracteristiques) : 1=hors agglomeration, 2=en ville
- `catr` (lieux) : type de route (1=autoroute, 4=voie communale...)
- `grav` (usagers) : gravite (1=indemne, 2=tue, 3=hospitalise, 4=blesse leger)

7 colonnes en tout, largement suffisant pour repondre à mes 3 questions pas besoin d'en prendre plus.

---

## 2. Le Pipeline Bronze / Silver / Gold

```
data/raw/*.csv                          --> (Bronze, CSV bruts)

        | 01_ingestion.py
        v
output/silver/caracteristiques/dep=*/   --> (Silver, Parquet partitionne par departement)
output/silver/lieux/
output/silver/usagers/
output/silver/vehicules/

        | 02_analyses.py
        v
output/gold/A1_gravite_meteo/           --> (Gold, resultats CSV)
output/gold/A2_accidents_par_route/
output/gold/A3_top_dep_par_mois/
```

### Pourquoi les Parquet plutot que les CSV ?

Un CSV ça ce lit ligne par ligne, meme si on a besoin que d'une colonne. Les parquet stocke les donnees par colonne si on veut juste la gravité par exemple --> `grav`, on lit uniquement ce bloc. En plus, le schema est stocker dans le fichier donc on n'a pas besoin de le re-declarer a chaque fois, et en plus la compression est meilleure. Question de bon sens plus qu'un choix complique en vrai.

### Pourquoi partitionnement par departement ?

On a ecrit la table `caracteristiques` en Parquet partitionne par `dep`. Ca cree un dossier par departement sur disque :

```
output/silver/caracteristiques/
    dep=75/  part-00000.parquet
    dep=13/  part-00000.parquet
    ...
```

Quand on filtre sur un departement precis, Spark va directement dans ce dossier et ignore les autres. On mesure l'effet de ce partitionnement dans l'etape 3 (partition pruning). Simple à mettre en place pour un gain pas negligeable, pas mal.

### Nettoyage (Doublon / Outliers / Null / Qualité de données)

- dropDuplicates sur Num_Acc (caracteristiques) : 54 402 --> 54 402 aucune ligne perdue
- dropDuplicates (usagers) : 125 187 --> 125 187 aucune ligne perdue
- codes atm hors [1-9] mis a null : 0 lignes concernees
- codes grav hors [1-4] mis a null : 0 lignes concernees

Aucun doublon detecte. Les donnees ONISR sont deja netoyée donc c'est beau, en même temp c'est ce qui est attendu pour un fichier officiel de data.gouv.fr. Le nettoyage a quand meme été fait car on ne sais jamais. c'est notre rôle de vérifier et s'assurer de la qualité des données, peut importe la source.

On a choisi de mettre les valeurs aberrantes à `null` et pas supprimer la ligne entière. Car un accident même avec une méteo inconnue ça reste utile pour l'analyse sur les types de routes par exemple. Petite decision perso, mais qui se defend je pense.

---



## 3. Les trois analyses

Petite precision avant de commencer --> j'ai fait mes 3 analyses en SQL plutot qu'avec l'API DataFrame de Spark. En perf ca change rien, les deux sont compiles vers le meme plan d'execution par Spark (Catalyst)donc pas de perte. C'est surtout que je suis plus a l'aise en SQL pour ecrire des jointures et des group by, et plus generalement pour tout ce qui touche aux requetes sur une base de donnees je prefere le SQL.

### Analyse 1 : la gravité selon la météo

Première question que je me suis posée --> est-ce que le mauvais temps rend vraiment les accidents plus mortels, ou c'est juste une idées que les films nous ont mis en têtes ? Pourrepondre j'ai joint `caracteristiques` (qui à la meteo, colonne `atm`) avec `usagers` (qui à la gravite, colonne `grav`) sur `Num_Acc`, et calculé le taux de tués par condition météo.

Voila la requete :

```sql
SELECT
    CASE c.atm WHEN 1 THEN 'Normal' WHEN 5 THEN 'Brouillard' ... END AS meteo,
    COUNT(*) AS nb_usagers,
    SUM(CASE WHEN u.grav = 2 THEN 1 ELSE 0 END) AS nb_tues,
    ROUND(SUM(CASE WHEN u.grav = 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS taux_tues_pct
FROM carac c
INNER JOIN usagers u ON c.Num_Acc = u.Num_Acc
WHERE c.atm IS NOT NULL AND u.grav IS NOT NULL
GROUP BY c.atm
ORDER BY taux_tues_pct DESC
```

Et ce que ça donne :
```
+-------------+----------+-------+---------------+-------------+
|meteo        |nb_usagers|nb_tues|nb_hospitalises|taux_tues_pct|
+-------------+----------+-------+---------------+-------------+
|Brouillard   |1306      |404    |49             |30.93        |
|Neige/grele  |613       |168    |46             |27.41        |
|Pluie forte  |3417      |790    |321            |23.12        |
|Vent fort    |364       |81     |23             |22.25        |
|Pluie legere |15491     |2972   |1272           |19.19        |
|Normal       |96243     |17463  |6917           |18.14        |
|Eblouissement|1824      |228    |259            |12.50        |
+-------------+----------+-------+---------------+-------------+
```

**Interprétations** : Le brouillard a le taux de tués le plus haut, 30.93%, presque le double du temps normal (18.14%). La neige et la pluie forte suivent juste derrière. Le temps normal reste de loin le plus frequent : 96 243 personnes concernees contre 1 306 pour le brouillard, mais ca tue moins en proportion. Ca colle avec la logique --> avec le brouillard on voit le danger trop tard, donc forcement l'impact est plus violent quand ça arrive. Mais ça veut pas dire que en periode de beau temp les accident ne sont pas grave, au total 17 463 morts par temps normal sur l'année, largement plus que toutes les autres conditions reunies.

---

### Analyse 2 : les routes les plus accidentogenes

Deuxieme réponse que je voulais avoir c'est est ce que ça arrive plus en ville ou sur la route ? 
Mon appriorie de depart est que les accident en ville sont plus courant mais moin graves que sur routes. 
J'ai croisé `caracteristiques` (colonne `agg`, urbain ou pas) avec `lieux` (colonne `catr`, le type de route).

```sql
SELECT
    CASE l.catr WHEN 1 THEN 'Autoroute' WHEN 4 THEN 'Voie communale' ... END AS type_route,
    CASE c.agg WHEN 2 THEN 'Agglomeration' ELSE 'Hors agglomeration' END AS milieu,
    COUNT(c.Num_Acc) AS nb_accidents
FROM carac c
INNER JOIN lieux l ON c.Num_Acc = l.Num_Acc
WHERE l.catr IS NOT NULL AND c.agg IS NOT NULL
GROUP BY l.catr, c.agg
ORDER BY nb_accidents DESC
```

```
+---------------------+------------------+------------+
|type_route           |milieu            |nb_accidents|
+---------------------+------------------+------------+
|Voie communale       |Agglomeration     |21233       |
|Route departementale |Hors agglomeration|10507       |
|Route departementale |Agglomeration     |10151       |
|Autoroute            |Hors agglomeration|4940        |
|Route nationale      |Hors agglomeration|2614        |
|Voie communale       |Hors agglomeration|1677        |
+---------------------+------------------+------------+
```

**Interprétations** : Les voies communales en agglo arrivent largement en tête, 21 233 accidents, presque le double de la categorie suivante. Logique, c'est les rues qu'on prend tous les jours avec des pietons et des intersections partout. L'autoroute par contre reste basse, seulement 4 940 accidents malgré des vitesses bien plus élevées. Le design de la route (glissières, pas de croisement, voies separées) compense le risque lié à la vitesse et la volonté des êtres humains de finir leurs vie plus vite que prévue. Les departementales hors agglo arrivent juste derriere les communales --> vitesse élevée, mais sans les sécurités de l'autoroute du coup ca fait des dégats.

---

### Analyse 3 : quels departements ressortent chaque mois (les plus accidentogenes)

Pour celle-la j'ai voulu tester une window function plutôt qu'un simple group by. Ca me permet de garder le detail par mois sans écraser les lignes. La question est : est-ce que ce sont toujours les mêmes departements qui ressortent, ou ça bouge selon la saison ? 
Peut etre que en ardeches les accident sont plus fréquent en hiver et que en gironde c'est plutot l'été. Sa nous permettrait aussi d'analyser un peu plus loin en fonction du department les type de route dominant, la méteo dominante etc.. et voir comparer par exemple une route national en ét en gironde est plus dangeruese qu'une route de montagne dans les pyrénée l'hiver (je pose juste la question).

Pas besoin de jointure ici, tout est deja dans `caracteristiques`. `DENSE_RANK()` classe les departements a l'interieur de chaque mois.

```sql
SELECT mois, dep, nb_accidents, rang
FROM (
    SELECT mois, dep, COUNT(Num_Acc) AS nb_accidents,
           DENSE_RANK() OVER (PARTITION BY mois ORDER BY COUNT(Num_Acc) DESC) AS rang
    FROM carac
    WHERE mois IS NOT NULL AND dep IS NOT NULL
    GROUP BY mois, dep
)
WHERE rang <= 5
ORDER BY mois, rang
```

Extrait, mois 1 et 2 :
```
+----+---+------------+----+
|mois|dep|nb_accidents|rang|
+----+---+------------+----+
|1   |75 |294         |1   |
|1   |93 |200         |2   |
|1   |92 |194         |3   |
|1   |13 |170         |4   |
|1   |94 |167         |5   |
|2   |75 |274         |1   |
|2   |92 |201         |2   |
|2   |93 |177         |3   |
|2   |13 |162         |4   |
|2   |94 |143         |5   |
+----+---+------------+----+
```

**Interprétations** : Paris (75) est numero 1 tous les mois, sans exception, je m'en doutais deja un peu. Le reste du top 5 bouge pas beaucoup non plus : Hauts-de-Seine, Seine-Saint-Denis, Val-de-Marne, Bouches-du-Rhone. L'Ile-de-France ecrase tout, saison ou pas. Ce qui m'a etonné, c'est qu'il y a aucun effet vacances : les Alpes, la cote d'Azur, ca n'apparait même pas en été. Ce qui compte c'est la densité de population et de trafic au quotidien, pas les périodes de vacances (même si les periodes de vacances engendre densité de traffic). La dessus, l'Ile-de-France c'est vraiment le champion de la ligue.

---

## 4. Optimisation (Broadcast Join)

Pour cette partie je voulais voir avec mes propres chiffres ce que le broadcast changeait vraiment, pas juste le lire dans un cours et hocher la tête. Test fait sur la jointure `caracteristiques` + `lieux` (54 402 lignes chacune, deux tables de taille comparable, idéal pour ce genre de comparaison).

Sans rien toucher, Spark fait un **SortMergeJoin** --> il redistribue les deux tables sur les coeurs selon `Num_Acc`, donc ça shuffle des deux côtés. Le `broadcast()` change ça => il envoie `lieux` en entier à chaque coeur direct, plus besoin de la bouger.

Pour que la mesure soit propre j'ai désactivé le broadcast automatique de Spark (`autoBroadcastJoinThreshold = -1`, sinon Spark l'aurait fait tout seul vu que `lieux` est petite) et j'ai mis les deux tables en cache avant de lancer le chrono pour pas mesurer la relectrue du parquet en plus du join.

Résultat, sans grande surprise sur la direction mais quand même impressionnant en vrai :
- join normal (SortMergeJoin) : 6.645s
- join broadcast (BroadcastHashJoin) : 0.603s
- gain =  **90.9%** plus rapide avec le broadcast

Je m'attendais à un gain, pas à un facteur x11 (dingue). Le plan d'execution confirme le changement de stratégie :

**Sans broadcast**, on voit deux `Exchange hashpartitioning` (un pour chaque table, donc le double du boulot) :
```
*(5) SortMergeJoin [Num_Acc], [Num_Acc], Inner
   :- *(2) Sort ...
   :  +- Exchange hashpartitioning(Num_Acc, 200)   <-- shuffle de caractéristique
   +- *(4) Sort ...
      +- Exchange hashpartitioning(Num_Acc, 200)   <-- shuffle de lieux
```

**Avec broadcast**, un seul `BroadcastExchange`, carac bouge pas du tout :
```
*(2) BroadcastHashJoin [Num_Acc], [Num_Acc], Inner, BuildRight
   :- *(2) Filter ...
   :  +- InMemoryTableScan ...                     <-- carac reste en place
   +- BroadcastExchange HashedRelationBroadcastMode <-- lieux envoyé à tous les coeurs
```

Le gain vient clairement de la suppression des deux shuffles. Je pense que sur un vrai cluster avec plusieurs machines l'écart serait encore plus grand forcemeent, vu que le shuffle implique du transfert réseau entre machines. En `local[*]` tout reste sur le même PC donc l'écart est déjà net, mais à l'échelle d'un cluster ce serait sans doute pire encore pour le SortMergeJoin.

---

## 5. Spark UI

**Un job avec shuffle, vu dans le DAG**

![DAG avec shuffle](Documentation/screenshots/dag_shuffle.png)

J'ai mis un peu de temps à comprendre ce que je regardais sur cette capture, prise pendant `01_ingestion.py`. La Stage 29 scanne le CSV et fait une première agrégation (`SortAggregate`), puis arrive un `Exchange` : c'est là que j'ai capté que c'était le shuffle déclenché par mon `dropDuplicates()` (il doit redistribuer les lignes par hash pour regrouper les doublons potentiel ensemble, logique une fois qu'on l'a compris). La Stage 30 prend le relais avec `AQEShuffleRead` (elle récupère ce que le shuffle vient d'écrire), refait un `SortAggregate` pour finaliser, puis `WriteFiles` écrit le résultat en Parquet. Dans le tableau des stages juste en dessous : 5/5 tasks terminées, 4.7 MiB lus en shuffle, 5.5 MiB écrits, 7s sur cette étape.

**Vue d'ensemble des jobs**

![Liste des jobs](Documentation/screenshots/jobs_list.png)

25 jobs au total sur cette exécution de `01_ingestion.py` : chaque `count()` et chaque écriture Parquet déclenche son propre job. La plupart durent moins d'une seconde vu le volume (quelques dizaines de milliers de lignes). Sur le coup je m'attendais à moins de jobs, mais visiblement chaque petite action compte.

---

## 6. Exploration : Partition Pruning et Predicate Pushdown

J'ai lancé `src/03_exploration.py` sur la couche Silver `caracteristiques` (partitionnée par `dep`, 54 402 accidents au total, un dossier par département sur le disque).

**Test 1, sans filtre** : je compte tous les accidents. Spark doit ouvrir tous les dossiers de département. Résultat : 3.635s. Le plan d'exécution confirme `PartitionFilters: []`, aucun filtre appliqué, tout est lu.

**Test 2, avec un filtre `dep='75'`** : 4 191 accidents à Paris, en 0.311s, soit **11.7x plus rapide** que le test 1. Le plan montre `PartitionFilters: [isnotnull(dep#15), (dep#15 = 75)]` : la preuve que Spark n'a ouvert que le dossier `dep=75/`, pas les autres.

**Test 3, filtre `dep='75'` + `atm=4`** (neige/grêle) : seulement 15 accidents, en 0.356s (logique, ça fait un croisement assez rare). Le plan montre les deux filtres en même temps : `PartitionFilters: [...dep=75]` pour le partition pruning, et en plus `PushedFilters: [IsNotNull(atm), EqualTo(atm,4)]`. La colonne `atm` n'est pas dans le nom du dossier comme `dep`, mais Spark arrive quand même à pousser ce filtre directement dans le lecteur Parquet, avant de charger les lignes en mémoire.

**Test 4, bonus AQE on/off** : 2.160s avec AQE contre 1.903s sans, sur un `groupBy(dep)`. Une petite différence, et pas dans le sens où je m'y attendais au départ (je pensais que l'AQE serait plus rapide vu que c'est censer optimiser). Sur ce volume (54 402 lignes) l'AQE ajuste le nombre de partitions de shuffle, mais l'avantage se voit surtout sur des gros volumes avec plein de partitions vides à éviter. Ici c'est trop petit pour que ça change grand chose.

**Ce que je retiens** : le partition pruning est de loin l'optimisation qui compte le plus ici, 11.7x plus rapide rien qu'en filtrant sur le bon département. Le predicate pushdown aide en plus sur les colonnes qui ne sont pas dans le partitionnement. Les deux sont automatiques dès que la couche Silver est bien partitionnée et que le filtre porte sur la bonne colonne, pas besoin de code spécial pour les activer.

---

## 7. Leçons et limites

### Ce que j'ai appris

--> Écrire le schéma à la main avec `StructType`, c'est long et un peu casse-tête (c'est le genre de partie du code qu'on peux faire générer pour gagner du temps, mais attention) mais obligatoire ici. Sans ça, `"01"` devient `1` et je me retrouve avec des bugs silencieux dans mes jointures, sans même m'en rendre compte au début.

--> La lazy evaluation, ça change la manière de coder. Je peux enchaîner `filter`, `join`, `groupBy` tant que je veux, rien ne se lance tant que je fais pas un `count()` ou un `write()`. C'est Spark qui choisit tout seul comment exécuter le tout à la fin.

--> Le broadcast join, c'est 6 secondes contre 0.6 sur ce volume. La différence vient du fait que `lieux` est assez petite pour tenir en mémoire, donc l'envoyer partout coûte moins cher que de shuffler `carac`.

--> La window function `DENSE_RANK()` classe par groupe sans perdre les lignes du détail. Un `GROUP BY` aurait tout regroupé et perdu l'info individuelle, la window function garde tout et rajoute juste le classement à côté. Petit détail qui change tout au final, une fois qu'on l'a compris on peut plus s'en passer.

### Difficultés rencontrées

--> Le setup Windows a été le premier obstacle, avant même de toucher au code métier : Spark refuse d'écrire quoi que ce soit tant que `HADOOP_HOME` et `winutils.exe` ne sont pas configurés. J'ai perdu pas mal de temps à chercher pourquoi mon script plantait à l'écriture du Parquet, avant de comprendre que c'est un problème classique sous Windows et pas un bug dans mon code.

--> La colonne `hrmn` (l'heure de l'accident) est stockée sous deux formats différents selon les lignes, "1430" et "14:30", sans raison apparente. J'ai dû gérer les deux avec deux regex séparées dans le même `withColumn`, sinon une bonne partie des heures passait à `null`.

--> Comprendre pourquoi rien ne s'affichait à l'écran tant que je n'appelais pas `.count()` ou `.show()` m'a pris un moment. La lazy evaluation de Spark, ça surprend la première fois qu'on la rencontre en vrai (en cours ça reste assez abstrait).

--> Et le fichier `usagers-2024.csv` avait une colonne en plus (`id_usager`) que mon schéma initial ne prévoyait pas. Repéré grâce à un warning de Spark au chargement, pas tout de suite évident à l'oeil nu vu le nombre de colonnes du fichier.

### Limites

On travaille sur une seule année (2024). Les observations sur la saisonnalité ou les tendances seraient plus solides avec plusieurs années, une année ca reste leger, pas une vari tendance.

Le fichier `usagers-2024.csv` à une colonne de plus que mon schéma initial (`id_usager`). Je l'ai détecté grâce au warning de Spark et corrigé. Ça montre que les schémas ONISR évoluent d'une année à l'autre, il faut vérifier l'en-tête avant de coder.

Tous les tests sont en mode `local[*]` sur une seule machine. Les gains mesurés (broadcast, partition pruning) seraient différents sur un vrai cluster Spark distribué où le réseau entre les machines est le vrai goulot d'étranglement.
