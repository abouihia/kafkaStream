**Cette présentation détaille le fonctionnement interne de Kafka Streams à travers trois concepts fondamentaux qui permettent d'assurer le parallélisme, la scalabilité et la tolérance aux pannes : les tâches, les threads et les instances.**

**1. Les Tâches (Tasks) : L'unité de travail**

**La ****tâche** est l'unité de travail la plus petite au sein d'une application Kafka Streams**. Elle exécute la topologie de traitement sur les événements entrants**.

* **Détermination du nombre :** Le nombre de tâches est directement lié au nombre de partitions des topics d'entrée**. Kafka Streams prend le nombre de partitions le plus élevé parmi les topics sources pour déterminer le nombre total de tâches**.
* **Assignation :** Chaque tâche correspond à une ou plusieurs partitions (une seule partition par topic source)**.**

**2. Les Threads (StreamThreads) : L'exécution**

**Les tâches sont assignées à des ****threads de flux** pour être exécutées**.**

* **Configuration par défaut :** Une application possède par défaut un seul thread qui traite chaque tâche l'une après l'autre**.**
* **Optimisation :** Il est possible d'augmenter le nombre de threads pour traiter les tâches en parallèle**. Une recommandation courante est d'utiliser environ ****deux fois le nombre de cœurs** de la machine locale pour définir le nombre de threads**.**
* **Limite :** On peut avoir autant de threads que de tâches. Tout thread supplémentaire resterait inactif par manque de travail à accomplir**.**

**3. Les Instances : La mise à l'échelle (Scaling)**

**Les ****instances** sont des copies distinctes de votre application fonctionnant avec le même `application ID`.

* **Distribution de la charge :** Ajouter des instances permet de répartir les tâches sur plusieurs processus ou machines, augmentant ainsi la puissance de traitement globale**.**
* **Haute disponibilité :** Contrairement aux threads, avoir une instance supplémentaire (même inactive) est utile pour le **failover**. Si une instance tombe en panne, une autre peut reprendre immédiatement les tâches vacantes sans attendre le redémarrage de l'instance défaillante**.**

**4. Élasticité et Protocole de Groupe**

**Le dynamisme de Kafka Streams repose sur l'utilisation interne du ****protocole de groupe de consommateurs** de Kafka**.**

* **Rééquilibrage (Rebalance) :** Lorsqu'une instance rejoint ou quitte le groupe, un "rebalance" se produit. Le broker Kafka coordonne alors une nouvelle distribution des tâches entre les membres actifs**.**
  * **Départ :** Si une instance s'arrête, ses tâches sont redistribuées aux membres restants**.**
  * **Arrivée :** Si une nouvelle instance arrive, les membres existants lui cèdent une partie de leurs ressources**.**
* **Continuité :** Ce processus est dynamique et s'effectue à l'exécution, souvent avec un délai très court, ce qui permet à l'application de s'adapter aux variations de trafic sans interruption majeure
