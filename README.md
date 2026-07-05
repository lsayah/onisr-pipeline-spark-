# Pipeline Spark ONISR 2024

Mon projet du jour 4 de la formation Spark. Je pars des CSV bruts d'accidents corporels ONISR 2024 (fournie par l'ETAT, lui même, l'unique), je nettoie tout ça (pas grand chose a néttoyer), je fais 3 analyses en SQL, une optimisation mesurée (broadcast join), et une exploration sur le partition pruning / predicate pushdown.

Le rapport complet avec toutes les mesures et les captures est ici : [rapport.md](rapport.md)

## Ce qu'il y a dans le repo

```
src/
  01_ingestion.py     -> lit les CSV bruts, nettoie, ecrit en Parquet (couche Silver)
  02_analyses.py      -> les 3 analyses SQL + la mesure du broadcast join, ecriture en Gold
  03_exploration.py   -> mon exploration sur le partition pruning / predicate pushdown
data/raw/             -> les CSV bruts ONISR (pas versionnes, a recuperer toi-meme, voir plus bas)
output/silver/        -> la couche Parquet nettoyée, partitionnée par département
output/gold/          -> les résultats des 3 analyses en CSV
rapport.md            -> le rapport complet
```

## Récupérer les données

1. Aller sur https://www.data.gouv.fr et chercher "bases de données annuelles accidents corporels"

2. Sélectionner l'année 2024

3. Télécharger les 4 fichiers CSV :
   - caract-2024.csv
   - lieux-2024.csv
   - vehicules-2024.csv
   - usagers-2024.csv

4. Les mettre dans `data/raw/` à la racine du projet

5. Lancer les scripts dans l'ordre, depuis la racine du projet :
   ```
   python src/01_ingestion.py
   python src/02_analyses.py
   python src/03_exploration.py
   ```

Notes et Remarque :
- séparateur --> point-virgule (;), pas une virgule
- encodage --> latin1, géré automatiquement par le script
- si c'est une autre année, faut changer les noms de fichiers directement dans les appels à `lire_csv()` de `src/01_ingestion.py` (pas de détection automatique)

ATTENTION :
En cas d'erreur Java/Spark sous Windows :
- vérifier que Java est installé : `java -version`
- si erreur liée à winutils/hadoop, voir le commentaire en haut de `src/01_ingestion.py` (HADOOP_HOME)
