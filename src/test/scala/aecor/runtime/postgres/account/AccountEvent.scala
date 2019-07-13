package aecor.runtime.postgres.account

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import io.circe.generic.auto._
import io.circe.jawn
import io.circe.syntax._

sealed abstract class AccountEvent extends Product with Serializable

object AccountEvent {
  case class AccountOpened(checkBalance: Boolean) extends AccountEvent

  case class AccountDebited(transactionId: TransactionId, amount: BigDecimal) extends AccountEvent

  case class AccountCredited(transactionId: TransactionId, amount: BigDecimal) extends AccountEvent

  implicit val serializer: Serializer[AccountEvent] = new Serializer[AccountEvent] {

    override def serialize(a: AccountEvent): (TypeHint, Array[Byte]) =
      ("", a.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))

    override def deserialize(typeHint: TypeHint, bytes: Array[Byte]): Either[Throwable, AccountEvent] =
      jawn
        .parseByteBuffer(ByteBuffer.wrap(bytes))
        .flatMap(_.as[AccountEvent])

  }
}
