import scala.collection.mutable

class Creature(val name : String, val category: String, val spellList: mutable.Buffer[String]) extends Serializable {
}