<<<<<<< HEAD
# SkyAnalytics #
## Prédiction et Optimisation de la Maintenance Aéronautique ##


Objectif du projet :
Vous travaillez pour une compagnie aérienne qui cherche à optimiser ses
opérations de maintenance. Votre mission est d'analyser les données de vol et de
maintenance pour prédire les besoins de maintenance futurs et leurs coûts
associés.


Avec:
Andrea, Ulrich, Guillaume et Patrick
# Top_gun
Projet 3 - Skyanalytics

D Voici le lien du dossier Drive du projet
Vous y retrouverez les 4 datasets de départ, la présentation générale ainsi que ce document

Objectifs principaux
-	Prédire l'état des voyants d'avertissement avec des données de vol actualisées.
-	Estimer les économies réalisées en anticipant les maintenances.
Données utilisées
-	Logs des vols (mise à jour chaque jour) : infos sur chaque vol comme durée, capteurs, état du voyant.
Format : logs_vols_AAAA-MM-JJ.csv
-	Dégradations (mise à jour chaque jour) : suivi de l’usure des composants et nécessité de remplacement.
Format : degradations_AAAA-MM-JJ.csv
-	Composants : détails sur chaque composant des avions (fixe - à mettre à jour vous-mêmes)
-	Aéronefs : informations sur chaque avion (fixe - à mettre à jour vous-mêmes)
Les premières données datent du 02 juin 2024
Récupération automatique des données
Les logs de vols et dégradations sont publiés chaque jour à cette adresse : http://sc-e.fr/docs/

Pour récupérer les fichiers d’une date spécifique (ex : 30 novembre 2023) :
-	Logs des vols : http://sc-e.fr/docs/logs_vols_2023-11-30.csv
-	Dégradations : http://sc-e.fr/docs/degradations_2023-11-30.csv
Tâches détaillées
1)	Script pour récupérer automatiquement ces fichiers chaque jour
2)	Mettre à jour les datasets composants et aéronefs selon les nouvelles données
3)	Analyser et visualiser l’évolution des dégradations et états des voyants
4)	Identifier des corrélations entre variables pour prédire les voyants
5)	Construire un modèle prédisant les voyants après chaque vol
Si voyant prédit, prévoir 3 jours d'immobilisation pour contrôles et changement des pièces à +75% d'usure
6)	Pour chaque vol, estimer le coût d'immobilisation selon le voyant
(1 jour = 15 000€ ; 2 = 7 jours ; 3 = 14 jours
+ prix des composants changés)
7)	Comparer les coûts totaux dans différents scénarios
8)	Optimiser le modèle pour maximiser les économies en maintenance
9)	Proposer des axes de développement futur
Livrables attendus
-	Notebook 1 / Document / Script : Automatisation et mise à jour datasets
-	Notebook 2 : Analyse, visualisation et préparation données
-	Notebook 3 : Modélisation, prédictions et calcul des coûts
Les notebooks doivent contenir un code PROPRE, structuré; organisé, et parfaitement commenté
-	Rapport détaillé des prédictions, état du parc et économies

La présentation, quant à elle, est totalement libre




Datasets : 
Dataset aeronefs :
        ref_aero : Référence unique de l'avion.
        type_model : Modèle de l'avion.
        debut_service : Date de mise en service de l'avion.
        last_maint : Date de la dernière maintenance.
        en_maintenance : Indique si l'avion est actuellement en maintenance.
        end_maint : Date prévue pour la fin de maintenance.

Dataset composants :
        ref_compo : Référence unique du composant.
        aero : Référence de l'avion associé.
        desc : Description du composant.
        lifespan : Durée de vie estimée du composant (en heures de vol).
        taux_usure_actuel : Taux d'usure actuel du composant.
        cout_composant : Coût du composant (en euros).

Dataset logs_vols :
        ref_vol : Référence unique du vol.
        aero_linked : Référence de l'avion lié au vol.
        jour_vol : Date du vol.
        time_en_air : Durée du vol en heures.
        sensor_data : Données des capteurs pendant le vol.
        etat_voyant : État du voyant (indicateur de problèmes potentiels).

Dataset degradation :
        ref_deg : Référence unique de l'enregistrement de dégradation.
        linked_aero : Référence de l'avion concerné.
        compo_concerned : Référence du composant concerné.
        usure_cumulée : Usure cumulée du composant.
        measure_day : Date de la mesure de l'usure.
        need_replacement : Indique si le composant a besoin d'être remplacé.

  
=======
# Top_gun
Projet 3 - Skyanalytics
>>>>>>> guillaume
