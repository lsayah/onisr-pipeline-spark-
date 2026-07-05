Telechargement des donnees ONISR 2024

1. Aller sur https://www.data.gouv.fr et chercher "bases de donnees annuelles
   accidents corporels"
2. Selectionner l'annee 2024
3. Telecharger les 4 fichiers CSV :
     - caract-2024.csv
     - lieux-2024.csv
     - vehicules-2024.csv
     - usagers-2024.csv
4. Les mettre dans data/raw/ a la racine du projet :
     data/
       raw/
         caract-2024.csv
         lieux-2024.csv
         vehicules-2024.csv
         usagers-2024.csv
5. Lancer les scripts dans l'ordre, depuis la racine du projet :
     python src/01_ingestion.py
     python src/02_analyses.py
     python src/03_exploration.py

Notes :
- separateur : point-virgule (;), pas une virgule
- encodage : latin1, gere automatiquement par le script
- si t'as une autre annee, faut changer les noms de fichiers directement
  dans les appels a lire_csv() de src/01_ingestion.py (pas de detection
  automatique)

En cas d'erreur Java/Spark sous Windows :
- verifier que Java est installe : java -version
- si erreur liee a winutils/hadoop, voir le commentaire en haut de
  src/01_ingestion.py (HADOOP_HOME)
