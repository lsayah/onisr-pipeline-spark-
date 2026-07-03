============================================================
  TELECHARGEMENT DES DONNEES ONISR 2022
============================================================

1. Aller sur : https://www.data.gouv.fr
   Chercher : "bases de donnees annuelles accidents corporels"

2. Selectionner l'annee 2022.

3. Telecharger les 4 fichiers CSV :
     - carcteristiques-2022.csv   (attention : faute de frappe officielle)
     - lieux-2022.csv
     - vehicules-2022.csv
     - usagers-2022.csv

4. Copier les 4 fichiers dans le dossier :
     Projet SPARK/data/raw/

   Apres copie, la structure doit etre :
     data/
       raw/
         carcteristiques-2022.csv
         lieux-2022.csv
         vehicules-2022.csv
         usagers-2022.csv

5. Lancer les scripts dans l'ordre :
     python 01_ingestion.py
     python 02_analyses.py
     python 03_exploration.py

============================================================
  NOTES IMPORTANTES
============================================================

- Separateur : point-virgule (;) — pas de virgule
- Encodage   : latin1 (gere automatiquement par le script)
- Annee      : on utilise 2022, les scripts cherchent *-2022.*

Si vous avez une autre annee (2021, 2023), changez l'annee
dans les fonctions trouver_fichier() des scripts.

============================================================
  EN CAS D'ERREUR JAVA / SPARK SUR WINDOWS
============================================================

Si vous voyez "JAVA_HOME not set" ou des erreurs Java :
  1. Verifier que Java est installe : java -version
  2. Si manquant, installer OpenJDK 17 depuis adoptium.net

Si vous voyez des erreurs liees a "winutils" ou "hadoop" :
  La plupart des operations locales fonctionnent sans winutils
  avec PySpark 3.5. Si vous avez des erreurs specifiques,
  notez le message exact et cherchez la solution.
