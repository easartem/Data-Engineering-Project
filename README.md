# Data-Engineering-Project


Project members : 

- BAGORIS Emeline
- BOERSMA Hélène


Question 6 : 

//TODO define average which takes an Iterator of Double in parameter \n
def average(values: Iterator[Double]): Option[Double] = \n
  if (values.size == 0) None \n
  else Some(values.foldLeft((0.0, 0)) { (acc, elem) => (acc._1+elem, acc._2+1) }).collect( elem => elem._1 / elem._2 )
