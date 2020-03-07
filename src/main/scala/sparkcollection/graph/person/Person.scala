package sparkcollection.graph.person

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
case class Person(name: String) {
  val friends = scala.collection.mutable.ArrayBuffer[Person]()

  def numberOfFriends() = friends.length

  def isFriend(other: Person) = friends.find(_.name == other.name)

  def isConnectedWithin2Steps(other: Person) = {
    for {f <- friends } yield {
      f.name == other.name || f.isFriend(other).isDefined
    }
  }.find(_ == true).isDefined

}
