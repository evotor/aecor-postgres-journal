package aecor.runtime.postgres.account

import aecor.encoding.{KeyDecoder, KeyEncoder}

final case class AccountId(value: String) extends AnyVal
object AccountId {
  implicit val keyEncoder: KeyEncoder[AccountId] = KeyEncoder.anyVal[AccountId]
  implicit val keyDecoder: KeyDecoder[AccountId] = KeyDecoder.anyVal[AccountId]
}
