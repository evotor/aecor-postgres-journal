package aecor.journal.postgres

final case class Offset(value: Long) extends AnyVal

object Offset {
  def zero: Offset = Offset(0L)
}
