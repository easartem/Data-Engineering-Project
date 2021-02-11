import scala.collection.MapView
import scala.io.BufferedSource

object TP_MR {
  /**
   * Answer question from 1 to 3
   * A) first using : match, case, ::, Nil or foldLeft
   * B) then using method of the List collection
   *
   * To run all the tests after your answer, type the command sbt test in the IntelliJ terminal
   * Example :
   * - type "sbt test" without the " in the terminal
   * - You will see that there is 11 TESTS FAILED
   * - Now write "Hello" instead of the ??? in the giveMeHelloString function
   * - rerun sbt test
   * - You will see  "Tests: succeeded 1, failed 10, canceled 0, ignored 0, pending 0" and if you scroll up you will see
   * - giveMeHelloString function in yellow/green which means that the test passed successfully
   *
   * Do this to check your answer after each exercise
   */

  def giveMeHelloString : String = "Hello"

  /**
   * 1) Find the last elements of a list.
   *
   * Example :
   * scala> last(List(1, 1, 2, 3, 5, 8))
   * res0: Option[Int] = Some(8)
   */

  //TODO define lastA using : match, case, ::, Nil
  def lastA[T](list: List[T]): Option[T] = list match {
    case head :: Nil => Some(head)
    case head :: tail => lastA(tail)
    case Nil => None}

  //TODO define lastB using method of the List collection
  def lastB[T](list: List[T]): Option[T] = list.lastOption


  /**
   * 2) Find the Kth element of a list:
   * By convention, the first element in the list is element 0.
   *
   * Example:
   * scala> nth(2, List(1, 1, 2, 3, 5, 8))
   * res0: Option[Int] = Some(2)
   */

  //TODO define nthA using : match, case, ::, Nil
  def nthA[T](x: Int, l: List[T]): Option[T] = (x, l) match {
    case (0, Nil) => None // We want element 0 but empty list
    case (0, x :: tail) => Some(x) // We want element 0 so we return the element in list with at least 1 element
    case (x, head :: tail) if x > 0 => nthA(x-1, tail) // We want element n so we return the element in list with at least 1 element by recursively going trough it
    case (x, list) => None // x > size of the list
  }

  //TODO define nthB using method of the List collection
  def nthB[T](x: Int, l: List[T]): Option[T] = l.lift(x)

  /**
   * 3) Reverse a list:
   *
   * Example:
   * scala> reverse(List(1, 1, 2, 3, 5, 8))
   * res0: List[Int] = List(8, 5, 3, 2, 1, 1)
   */

  //TODO define reverseA using : foldLeft, ::
  //  println(List(1, 2, 3, 4, 5).foldLeft(List.empty[Int])((acc, el) => el :: acc))
  def reverseA[T](list: List[T]): List[T] = list match {
    case h :: tail => reverseA(tail) ::: List(h)
    case Nil => Nil
  }

  //TODO define reverseB using method of the List collection
  def reverseB[T](list: List[T]): List[T] = list.reverse

  /**
   * 4) Sum of wages:
   * With the case class Employee defined below, find the sum of salaries from a list of employees
   *
   * Example:
   * scala> salarySum(List(Employee("Jon", 2000), Employee("Jane", 3500)))
   * res0: Double = 5500.0
   */

  case class Employee(name: String, salary: Double)

  //TODO define salarySum which is the sum of the salaries from a list of employees
  def salarySum(employees: List[Employee]): Double = {
    employees.map(employee => employee.salary).sum
  }


  /**
   * 5) Address list:
   * With the case class User defined below, list the all their addresses
   *
   * Example:
   * scala> addressOf(List(User("Jon", "5 Av. des Champs-Élysées, Paris"), User("James","17 Boulevard Poissonnière, Paris")))
   * res0: List[String] = List("5 Av. des Champs-Élysées, Paris","17 Boulevard Poissonnière, Paris")
   */

  case class User(name: String, address: String)

  //TODO define addressOf which gives a list of addresses from a list of users
  def addressOf(users: List[User]): List[String] = users match {
    case s :: rest => s.address :: addressOf(rest)
    case Nil => Nil
  }

  /**
   * 6) Define the average function :
   *
   * Example:
   * scala> average(Iterator(1, 2, 3, 4, 5, 6, 7, 8))
   * res0: Option[Double] = Some(4.5)
   */

  //TODO define average which takes an Iterator of Double in parameter
  def average(values: Iterator[Double]): Option[Double] =
    if (values.size == 0) None
    else Some(values.foldLeft((0.0, 0)) { (acc, elem) => (acc._1+elem, acc._2+1) }).collect( elem => elem._1 / elem._2 )

  /**
   * 7) Monoids and almost MapReduce
   *
   * /////////////////////
   * Extract from an article :
   *
   * "un monoïde est une structure algébrique correspondant à un ensemble (les valeurs) avec un élément neutre (la valeur initiale)
   * et une loi de composition interne associative (l'opération binaire).
   * C'est souvent noté (E, ✻, e), avec E l'ensemble de valeurs, ✻ la loi de composition interne et e l'élément neutre.
   *
   * Alors, on dit loi de composition, car l'idée de notre opération est de combiner deux éléments de notre ensemble de valeurs
   * (ie. de les composer, d'ailleurs on devrait pouvoir parler d'agréger des valeurs).
   * On dit interne car en combinant ces deux valeurs, notre opération retourne une nouvelle valeur
   * qui fait partie de notre ensemble initial de valeurs (ie. on ne sort pas de cet ensemble).
   *
   * Pour associative, hé bien... Si nous utilisons plusieurs fois notre opération dans une expression,
   * il est alors possible d'évaluer l'expression quelque soit l'endroit où on commence à l'évaluer.
   * C'est-à-dire qu'avec l'expression a + b + c, je peux très bien commencer par x = a + b
   * et ensuite faire x + c ou commencer par x = b + c et faire a + x après.
   * Au final, on note ça (a + b) + c = a + (b + c). L'ordre d'évaluation des sous-expressions n'a pas d'importance pour un monoïde.
   *
   * C'est une propriété intéressante, car ça permet de découper une expression en sous-expression et d'évaluer ces sous-expressions en parallèle,
   * puis de récupérer et combiner les résultats de ces évaluations pour obtenir le résultat final...
   * Et comme ça, on vient de réinventer MapReduce !"
   *
   *
   *
   * Voici une liste de stations de ski avec notamment leur localisation et une évaluation sur 5.
   *
   * Nous avons ci-dessous un extrait d'une liste des stations de ski en France.
   * Bon... Ce n'est pas du big data, hein ! Mais on va faire comme si 😬.
   *
   * Cette liste n'est pas complète en terme d'information.
   * Malgré ça, nous allons donner la moyenne des évaluations... tel que le ferait MapReduce ou Spark.
   *
   * Dans MapReduce, il y a une étape de préparation des données où on part d'un fichier pour le convertir en un ensemble clé/valeur.
   *
   * /////////////////////
   */

  /*TODO
     Now, define getRatingsByDepartement, imagine reading the file "ski_stations_ratings.csv" in the folder Resources and getting an Iterable[String] for each row
     Note that the rating is in position 1 in each line and the department in position 6
   */
  import scala.util.Try

  // we want to get an Map where the key is departement and the value an iterable of all the ratings of that departement
  def getRatingsByDepartement(lines: Iterable[String]): Map[String, Iterable[Double]] = {
    // drop CSV header
    val data: Iterable[String] = lines.drop(1)

    val rows: Iterable[Array[String]] =
      data.map { line =>
        //TODO cleaning line and separate fields by the comma character
        val row: Array[String] = ???

        // cleansing: if fields are missing, we pad row with empty strings
        row.padTo(7, "")
      }

    // we want an Iterable consisting of the pair Departement and Rating
    val deptRatings: Iterable[(String, Double)] =
      //TODO we remove lines with no departement
      ???
        //then we map the creation of the tuple, just uncomment
        /*.map(fields =>
          (fields(6), Try { fields(1).toDouble }.getOrElse(0.0))
        )*/

    deptRatings
      .groupBy { case (departement, rating) => departement }
      .view.mapValues(row => row.map { case (departement, rating) => rating }).toMap
  }

  /**
   * /////////////////////
   * From the article:
   *
   * La fonction getRatingsByDepartement exécute en fait un shuffle (ie. une redistribution des données)
   * en réalisant un partitionnement utilisant le département comme clé (ce qui n'est pas la meilleure des clés,
   * dans la mesure où la répartition des données dans les différentes partitions sera ici déséquilibrée,
   * puisque par exemple dans les Vosges il n'y a pas beaucoup de stations contrairement à la Haute-Savoie...
   * Mais, bon. Ce n'est comme si on pouvait faire du big data avec l'énumération des stations de ski en France).
   * Ici, à chaque clé correspond une partition des évaluations.
   * Dans le cadre de MapReduce, chaque partition serait déposée dans des nœuds différents du cluster.
   *
   * Il va maintenant falloir calculer la moyenne. En supposant, qu'on ait à faire à une liste immense,
   * il est plus intéressant de parcourir cette liste en une seule passe qu'en deux.
   * Car pour calculer une moyenne, il faut d'un côté une somme de valeurs et de l'autre leur quantité, avant de diviser ces deux résultats.
   * Ce qui normalement implique deux passes sur notre dataset.
   * Pour le faire en une seule passe, nous allons calculer la somme et la quantité en même temps,
   * en stockant les résultats intermédiaires dans un couple de valeurs (somme, quantité).
   *
   * Alors, il existe différentes approches pour implémenter ce calcul de moyenne.
   * Pour l'exercice ici, nous allons étudier une solution mettant en avant la notion de monoïde,
   * en se basant sur une typeclasse (un peu à la manière de la bibliothèque Scala Cats).
   *
   * En Scala, pour déclarer une typeclasse Monoid, il faut déclarer un trait générique,
   * où le paramètre A représente le type qui sera qualifié de monoïde.
   * Ce trait contient deux méthodes empty qui retourne l'élément neutre et combine qui permet de combiner deux éléments de A.
   *
   * /////////////////////
   */

  trait Monoid[A] {
    def empty: A
    def combine(a: A, b: A): A
  }

  object Monoid {
    //this will allows us to write Monoid[Int].empty instead of implicitly[Monoid[Int]].empty
    @inline def apply[A](implicit ev: Monoid[A]): Monoid[A] = ev
  }

  /**
   * Let's declare a few instances of our typeclass Monoid, that will help us solve our problem
   */

  // TODO Monoid (Int, +, 0)
  implicit val intMonoid: Monoid[Int] = new Monoid[Int] {
    override def empty: Int = ???
    override def combine(a: Int, b: Int): Int = ???
  }

  // TODO Monoid (Double, +, 0.0)
  implicit val doubleMonoid: Monoid[Double] = new Monoid[Double] {
    override def empty: Double = ???
    override def combine(a: Double, b: Double): Double = ???
  }

  // TODO turn any tuple (A, B) into Monoid, providing A and B both are Monoid
  implicit def tupleMonoid[A: Monoid, B: Monoid]: Monoid[(A, B)] =
    new Monoid[(A, B)] {
      override def empty: (A, B) = ???

      override def combine(left: (A, B), right: (A, B)): (A, B) =
        ???
    }

  /**
   * Let's add to some collection the operation combineAll, in which if the values of the collection are from a monoid,
   * combine all the values in order to have only one result
   */

  implicit class iterableWithCombineAll[A: Monoid](l: Iterable[A]) {
    def combineAll: A = l.fold(Monoid[A].empty)(Monoid[A].combine)
  }

  //our MapReduce program/function
  def skiRatingAverage:Double = {
    /**
     * Let's use our function getRatingsByDepartement with a file to obtain a partionning of the data
     */

    import scala.io.Source

    val file: BufferedSource = Source.fromFile("resources/ski_stations_ratings.csv")
    val partitions: Map[String, Iterable[Double]] = getRatingsByDepartement(file.getLines().to(Iterable))

    /**
     * And now let's do our MapReduce
     */

    // TODO phase 1 (Map): get ratings only and associate the value 1 to the rating (create a pair (rating,1))
    val partitionedRatingWithOne: MapView[String, Iterable[(Double, Int)]] =
      ???

    // TODO phase 2 (Combine): locally sum ratings and 1s for each partition
    val partitionedSumRatingsAndCount: MapView[String, (Double, Int)] =
      ???

    // TODO phase 3 (Reduce): combine for all partitions the sum of ratings and counts
    val (rating, count) : (Double,Int) =
      ???

    rating / count
  }

}
