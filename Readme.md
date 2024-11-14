# TP Spark

## Checkpoint Alpha


---

## 1. Chargement et vérification du fichier

Pour chager le fichier `AOL-01.txt` 

```scala
val aolRdd = sc.textFile("AOL-01.txt")
```

Vérification du bon chargement des données:

- Nombre de lignes dans le RDD : `aolRdd.count()`
- Exemple de 5 premières lignes :

```scala
aolRdd.take(5).foreach(println)
```

---

## 2. Recherche des requêtes contenant le mot "google"


```scala
val googleLines = aolRdd.filter(line => line.toLowerCase.contains("google"))
```

- **Nombre de lignes contenant "google"** : `googleLines.count()`
- **Exemple de 5 requêtes contenant "google"** :

```scala
googleLines.take(5).foreach(println)
```

---

## 3. Extraction des requêtes

Chaque ligne est découpée par des tabulations (`\t`), et la requête (Query) se trouve à l'index 1 :

```scala
val queriesRdd = aolRdd.map(line => line.split("\t")(1))
```

- **Nombre total de requêtes** : `queriesRdd.count()`
- **Exemple de 5 requêtes extraites** :

```scala
queriesRdd.take(5).foreach(println)
```

---

## 4. Recherche des requêtes avec mots répétés
Pour identifier les requêtes contenant des mots répétés, on peut comparer la taille de la liste de mots et celle du Set de mots (qui élimine les doublons) :

```scala
val repeatedQueriesRdd = queriesRdd.filter(query => {
  val words = query.toLowerCase.split(" ").toList
  words.length > words.toSet.size
})
```

**Statistiques sur les requêtes avec mots répétés** :

- Nombre total de requêtes : `totalQueries`
- Nombre de requêtes avec mots répétés : `repeatedQueries`
- Pourcentage de requêtes avec mots répétés : `percentage`

```scala
val repeatedQueryCount = repeatedQueriesRdd.count()
```
```scala
val totalQueryCount = queryRdd.count()
```
```scala
val percentage = (repeatedQueryCount.toDouble / totalQueryCount) * 100
```
```scala
println(f"Pourcentage de requêtes avec répétition de mot : $percentage%.2f%%")
```


---

## 5. Analyse des mots les plus recherchés

On transforme chaque requête en une liste de mots uniques, puis on compte les occurrences de chaque mot :

```scala
val uniqueWordsRdd = queriesRdd.flatMap { query =>
  query.split(" ").map(_.toLowerCase).toSet
}

val wordCounts = uniqueWordsRdd.map(word => (word, 1)).reduceByKey(_ + _)
val sortedWordCounts = wordCounts.sortBy(_._2, ascending = false)
val top50Words = sortedWordCounts.take(50)
```

**Top 50 des mots les plus recherchés** :

```scala
top50Words.foreach { case (word, count) =>
  println(s"$word : $count")
}
```

---

## 6. Analyse des tailles des requêtes

On recupère la longueur des requêtes en termes de nombre de mots. Ensuite on compte la fréquence de chaque taille de requête 

```scala
val queryLengthsRdd = queriesRdd.map(query => query.split(" ").length)
val lengthCounts = queryLengthsRdd.map(length => (length, 1)).reduceByKey(_ + _)
val sortedLengthCounts = lengthCounts.sortBy(_._1, ascending = false)
val top20Lengths = sortedLengthCounts.take(20)
```

**Top 20 des tailles de requêtes** (en nombre de mots) :

```scala
top20Lengths.foreach { case (length, count) =>
  println(s"Taille de la requête : $length mots, Nombre d'occurrences : $count")
}
```

## Checkpoint Bravo

---

## 7. Suggestions de mots associés

Pour chaque requête, on divise d'abord le texte en une liste de mots, en retirant les doublons au sein d'une requête afin de ne pas surévaluer les co-occurrences dans des phrases où un mot serait répété plusieurs fois.

```scala
val wordsInQueries = queriesRdd.map(query => query.toLowerCase.split(" ").distinct)

En utilisant des « list comprehensions » (compositions de listes) en Scala, on génère toutes les paires de mots possibles pour chaque requête. Cela permet de créer des paires (mot1, mot2) et (mot2, mot1) pour chaque couple de mots apparaissant ensemble dans une requête. L’objectif est de préparer la structure (mot, mot associé), avec 1 comme valeur pour représenter chaque co-occurrence.

val wordPairs = wordsInQueries.flatMap { words =>
  for {
    word <- words
    otherWord <- words if word != otherWord
  } yield ((word, otherWord), 1)
}

À ce stade, on a un RDD de paires avec leur co-occurrence sous la forme ((mot, mot associé), 1). En utilisant reduceByKey, on peut regrouper les valeurs pour obtenir le nombre total de co-occurrences pour chaque paire (mot, mot associé).

val wordPairCounts = wordPairs.reduceByKey(_ + _)


Pour obtenir une structure où chaque mot est associé à une liste de ses mots associés avec leur fréquence, on modifi le RDD pour regrouper les paires (mot, mot associé) par le premier mot, ce qui permettra ensuite de trier les associations par fréquence.


val wordToAssociates = wordPairCounts.map { case ((word, associate), count) =>
  (word, (associate, count))
}


Enfin, on regroupes les associations pour chaque mot et on tri les mots associés par fréquence décroissante, en prenant seulement les 5 premières.


val topAssociatesByWord = wordToAssociates.groupByKey().mapValues { associates =>
  associates.toList.sortBy(-_._2).take(5)
}
```

On peut filter pout afficher le resultat

**Exemple pour le mot "france"** :

```scala
topAssociatesByWord.filter(_._1 == "france").collect().foreach(println)
```

---

## Checkpoint Charlie
## FIN

