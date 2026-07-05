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

Le fait qu'il y a plus de lignes dans usagers que dans caracteristiques est normal : un seul accident peut logiquement impliquer plusieurs personnes.

Les colonnes cles que j'ai utilisees :
- `Num_Acc` (dans toutes les tables) : identifiant de l'accident, cle de jointure
- `atm` (caracteristiques) : conditions meteo (1=normal, 5=brouillard...)
- `dep` (caracteristiques) : departement
- `mois` (caracteristiques) : mois de l'accident
- `agg` (caracteristiques) : 1=hors agglomeration, 2=en ville
- `catr` (lieux) : type de route (1=autoroute, 4=voie communale...)
- `grav` (usagers) : gravite (1=indemne, 2=tue, 3=hospitalise, 4=blesse leger)

---

## 2. Pipeline Bronze / Silver / Gold

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

Un CSV ça ce lit ligne par ligne, meme si on a besoin que d'une colonne. Les parquet stocke les donnees par colonne si on veut juste la gravité par exemple --> `grav`, on lit uniquement ce bloc. En plus, le schema est stocker dans le fichier donc on n'a pas besoin de le re-declarer a chaque fois, et en plus la compression est meilleure.

### Pourquoi partitionnement par departement ?

On a ecrit la table `caracteristiques` en Parquet partitionne par `dep`. Ca cree un dossier par departement sur disque :

```
output/silver/caracteristiques/
    dep=75/  part-00000.parquet
    dep=13/  part-00000.parquet
    ...
```

Quand on filtre sur un departement precis, Spark va directement dans ce dossier et ignore les autres. On mesure l'effet de ca dans l'etape 3 (partition pruning).

### Nettoyage (Doublon / Outliers / Null / Qualité de données)

- dropDuplicates sur Num_Acc (caracteristiques) : 54 402 -> 54 402, aucune ligne perdue
- dropDuplicates (usagers) : 125 187 -> 125 187, aucune ligne perdue
- codes atm hors [1-9] mis a null : 0 lignes concernees
- codes grav hors [1-4] mis a null : 0 lignes concernees

Aucun doublon detecte. Les donnees ONISR sont deja netoyée donc c'est beau, en même temp c'est ce qui est attendu pour un fichier officiel de data.gouv.fr. Le nettoyage a quand meme été fait car on ne sais jamais. c'est notre rôle de verifier et s'assurer de la qualité des données, peut importe la source.

On a choisi de mettre les valeurs aberrantes a `null` et pas supprimer la ligne entiere. Car un accident même avec une meteo inconnue ça reste utile pour l'analyse sur les types de routes par exemple.

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

**Interprétations** : Le brouillard a le taux de tués le plus haut, 30.93%, presque le double du temps normal (18.14%). La neige et la pluie forte suivent juste derriere. Le temps normal reste de loin le plus frequent : 96 243 personnes concernees contre 1 306 pour le brouillard, mais ca tue moins en proportion. Ca colle avec la logique : avec le brouillard on voit le danger trop tard, donc l'impact est plus violent quand ca arrive. Ca veut pas dire que le beau temps est sans danger pour autant : en nombre total, 17 463 morts par temps normal sur l'annee, largement plus que toutes les autres conditions reunies.

---

### Analyse 2 : les routes les plus accidentogenes

Deuxieme reponse que je voulais avoir c'est est ce que ça arrive plus en ville ou sur la route ? 
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

**Interprétations** : Les voies communales en agglo arrivent largement en tete, 21 233 accidents, presque le double de la categorie suivante. Logique, ce sont les rues qu'on prend tous les jours, avec des pietons et des intersections partout. L'autoroute par contre reste basse : seulement 4 940 accidents malgre des vitesses bien plus elevees. Le design de la route (glissieres, pas de croisement, voies separees) compense le risque lié a la vitesse. Les departementales hors agglo arrivent juste derriere les communales : vitesse elevee, mais sans les securites de l'autoroute, du coup ca fait des degats.

---

### Analyse 3 : quels departements ressortent chaque mois (les plus accidentogenes)

Pour celle-la j'ai voulu tester une window function plutot qu'un simple group by, histoire de garder le detail par mois sans ecraser les lignes. La question est : est-ce que ce sont toujours les memes departements qui ressortent, ou ça bouge selon la saison ? 
Peut etre que en ardeches les accident sont plus fréquent en hiver et que en gironde c'est plutot l'été. Sa nous permettrait aussi d'analyser un peu plus loin en fonction du department les type de route dominant, la meteo dominante etc.. et voir comparer par exemple une route national en ét en gironde est plus dangeruese qu'une route de montagne dans les pyrénée l'hiver (je pose juste la question).

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

**Interprétations** : Paris (75) est numero 1 tous les mois, sans exception, je m'en doutais deja un peu. Le reste du top 5 bouge pas beaucoup non plus : Hauts-de-Seine, Seine-Saint-Denis, Val-de-Marne, Bouches-du-Rhone. L'Ile-de-France ecrase tout, saison ou pas. Ce qui m'a etonné, c'est qu'il y a aucun effet vacances : les Alpes, la cote d'Azur, ca n'apparait meme pas en ete. Ce qui compte c'est la densite de population et de trafic au quotidien, pas les periodes de vacances. La dessus, l'Ile-de-France c'est vraiment le champion de la ligue.

---

## 4. Optimisation (Broadcast Join)

Pour cette partie j'ai voulu mesurer concretement l'effet du broadcast join, sur la jointure `caracteristiques` + `lieux` (54 402 lignes chacune, deux tables assez comparables en taille donc ça se pretait bien au test).

Par defaut Spark fait un **SortMergeJoin** : il redistribue les deux tables sur les coeurs CPU selon la cle de jointure, donc shuffle des deux cotes. Avec `broadcast()`, on envoie `lieux` en entier a chaque coeur, plus besoin de la shuffler.

Pour que la mesure soit propre j'ai desactive le broadcast automatique de Spark (`autoBroadcastJoinThreshold = -1`, sinon Spark l'aurait fait tout seul vu que `lieux` est petite) et mis les deux tables en cache avant de lancer le chrono, histoire de pas mesurer la relecture du Parquet en plus du join.

Resultat, sans grande surprise sur la direction mais quand meme impressionnant en vrai :
- join normal (SortMergeJoin) : 6.645s
- join broadcast (BroadcastHashJoin) : 0.603s
- gain : **90.9%** plus rapide avec le broadcast

Je m'attendais a un gain, pas a un facteur x11. Le plan d'execution confirme le changement de strategie :

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
   +- BroadcastExchange HashedRelationBroadcastMode <-- lieux envoye a tous les coeurs
```

Le gain vient clairement de la suppression des deux shuffles. Et je pense que sur un vrai cluster avec plusieurs machines l'ecart serait encore plus marque, vu que le shuffle implique du transfert reseau entre machines. En `local[*]` tout reste sur le meme PC donc l'ecart est deja net, mais a l'echelle d'un cluster ce serait sans doute pire encore pour le SortMergeJoin.

---

## 5. Spark UI

**Un job avec shuffle, vu dans le DAG**

![DAG avec shuffle](Documentation/screenshots/dag_shuffle.png)

Cette capture vient de l'execution de `01_ingestion.py`. La Stage 29 fait le scan du CSV et une premiere agregation (`SortAggregate`), puis un `Exchange` : c'est le shuffle declenche par `dropDuplicates()`, qui doit redistribuer les lignes par hash pour regrouper les eventuels doublons ensemble. La Stage 30 prend le relai avec `AQEShuffleRead` (elle lit ce que le shuffle vient d'ecrire), refait un `SortAggregate` pour finaliser, puis `WriteFiles` ecrit le resultat en Parquet. Dans le tableau des stages en dessous : 5/5 tasks terminees, 4.7 MiB lus en shuffle, 5.5 MiB ecrits, pour 7s sur cette etape.

**Vue d'ensemble des jobs**

![Liste des jobs](Documentation/screenshots/jobs_list.png)

25 jobs au total sur cette execution de `01_ingestion.py` : chaque `count()` et chaque ecriture Parquet declenche son propre job. La plupart durent moins d'une seconde vu le volume (quelques dizaines de milliers de lignes).

---

## 6. Exploration : Partition Pruning et Predicate Pushdown

*(a completer avec les vrais chiffres apres avoir lance `03_exploration.py` et capture les metriques "number of files read" dans l'onglet SQL/DataFrame)*

---

## 7. Ce qu'on a appris et les limites

### Ce qu'on a appris

Ecrire le schema a la main avec `StructType`, c'est long et un peu casse tête (c'est le genre de partie du code qu'on peux faire generer pour gagner du temp, mais attention) mais obligatoire ici. Sans ca, `"01"` devient `1` et je me retrouve avec des bugs silencieux dans mes jointures, sans meme m'en rendre compte au debut.

La lazy evaluation, ca change la maniere de coder. Je peux enchainer `filter`, `join`, `groupBy` tant que je veux, rien ne se lance tant que je fais pas un `count()` ou un `write()`. C'est Spark qui choisit tout seul comment executer le tout a la fin.

Le broadcast join, c'est 6 secondes contre 0.6 sur ce volume. La difference vient du fait que `lieux` est assez petite pour tenir en memoire, donc l'envoyer partout coute moins cher que de shuffler `carac`.

La window function `DENSE_RANK()` classe par groupe sans perdre les lignes du detail. Un `GROUP BY` aurait tout regroupe et perdu l'info individuelle, la window function garde tout et rajoute juste le classement a cote.

### Difficultes rencontrees

Le setup Windows a ete le premier obstacle, avant meme de toucher au code metier : Spark refuse d'ecrire quoi que ce soit tant que `HADOOP_HOME` et `winutils.exe` ne sont pas configures. J'ai perdu pas mal de temps a chercher pourquoi mon script plantait a l'ecriture du Parquet, avant de comprendre que c'est un probleme classique sous Windows et pas un bug dans mon code.

La colonne `hrmn` (l'heure de l'accident) est stockee sous deux formats differents selon les lignes, "1430" et "14:30", sans raison apparente. J'ai du gerer les deux avec deux regex separees dans le meme `withColumn`, sinon une bonne partie des heures passait a `null`.

Comprendre pourquoi rien ne s'affichait a l'ecran tant que je n'appelais pas `.count()` ou `.show()` m'a pris un moment. La lazy evaluation de Spark, ca surprend la premiere fois qu'on la rencontre en vrai (en cours ca reste assez abstrait).

Et le fichier `usagers-2024.csv` avait une colonne en plus (`id_usager`) que mon schema initial ne prevoyait pas. Repere grace a un warning de Spark au chargement, pas tout de suite evident a l'oeil nu vu le nombre de colonnes du fichier.

### Limites

On travaille sur une seule annee (2024). Les observations sur la saisonnalite ou les tendances seraient plus solides avec plusieurs annees.

Le fichier `usagers-2024.csv` a une colonne de plus que mon schema initial (`id_usager`). Je l'ai detecte grace au warning de Spark et corrige. Ca montre que les schemas ONISR evoluent d'une annee a l'autre, il faut verifier l'en-tete avant de coder.

Tous les tests sont en mode `local[*]` sur une seule machine. Les gains mesures (broadcast, partition pruning) seraient differents sur un vrai cluster Spark distribue ou le reseau entre les machines est le vrai goulot d'etranglement.
