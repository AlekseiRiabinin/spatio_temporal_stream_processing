package phd.architecture.model

import org.apache.flink.api.common.typeinfo.TypeInformation


object TypeInfos {

  implicit val eventTypeInfo: TypeInformation[Event] =
    TypeInformation.of(classOf[Event])

  implicit val spatialPartitionTypeInfo: TypeInformation[SpatialPartition] =
    TypeInformation.of(classOf[SpatialPartition])

  implicit val windowResultTypeInfo: TypeInformation[WindowResult] =
    TypeInformation.of(classOf[WindowResult])
}
