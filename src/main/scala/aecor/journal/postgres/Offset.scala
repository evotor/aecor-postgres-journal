package aecor.journal.postgres

final case class Offset(value: Long) extends AnyVal {
  def increment: Offset = Offset(value + 1L)
}

object Offset {
  def zero: Offset = Offset(0l)
}
