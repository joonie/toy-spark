package sparkcollection.graph.person

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
object personMain {
  def main(args: Array[String]): Unit = {
    val john = Person("Jone")
    val ken = Person("Ken")
    val mary = Person("Marry")
    val dan = Person("Dan")


    println(john.numberOfFriends())

    john.friends += ken
    println(john.numberOfFriends())

    ken.friends += mary
    println(ken.numberOfFriends())

    mary.friends += dan
    println(mary.numberOfFriends())

    println(john.isFriend(ken))
    println(john.isFriend(mary))
    println(john.isFriend(dan))

    println(john.isConnectedWithin2Steps(ken))
    println(john.isConnectedWithin2Steps(mary))
    println(john.isConnectedWithin2Steps(dan))
  }

}
