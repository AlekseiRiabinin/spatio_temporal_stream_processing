package phd.streammodels.model

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation


object TypeInfos {

  implicit val eventTypeInfo: TypeInformation[Event] =
    createTypeInformation[Event]

  /**
    * Generic TypeInformation for WindowResult[K].
    * Works as long as TypeInformation[K] is available.
    */
  implicit def windowResultTypeInfo[K : TypeInformation]: TypeInformation[WindowResult[K]] =
    createTypeInformation[WindowResult[K]]
}
