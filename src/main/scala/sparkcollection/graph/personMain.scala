package sparkcollection.graph

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
object personMain {
  def main(args: Array[String]): Unit = {
    val john = Person("Jone")
    val ken = Person("Ken")
    val mary = Person("Marry")
    val dan = Person("Dan")

    john.numberOfFriends()

    john.friends += ken
    john.numberOfFriends()

    ken.friends += mary
    ken.numberOfFriends()

    mary.friends += dan
    mary.numberOfFriends()

    john.isFriend(ken)
    john.isFriend(mary)
    john.isFriend(dan)

    john.isConnectedWithin2Steps(ken)
    john.isConnectedWithin2Steps(mary)
    john.isConnectedWithin2Steps(dan)
  }

}
