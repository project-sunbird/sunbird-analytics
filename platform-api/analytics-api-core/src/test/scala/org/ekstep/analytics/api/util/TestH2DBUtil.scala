package org.ekstep.analytics.api.util

import org.ekstep.analytics.api.BaseSpec
import java.sql.{Connection, PreparedStatement, ResultSet}

import org.mockito.Mockito._

class TestH2DBUtil extends BaseSpec {

  val connectionMock = mock[Connection]
  val preparedStatementMock = mock[PreparedStatement]
  val resultSetMock = mock[ResultSet]

  val H2DB = new H2DBUtil() {
    override def getDBConnection = connectionMock
  }

  "H2DB util: readLocation" should "return state and district info" in {
    val sqlString = "Select * from geo_location_city_ipv4"
    when(connectionMock.prepareStatement(sqlString)).thenReturn(preparedStatementMock)
    when(preparedStatementMock.executeQuery()).thenReturn(resultSetMock)
    when(resultSetMock.getString("state")).thenReturn("Karnataka")
    when(resultSetMock.getString("district_custom")).thenReturn("KA")
    when(resultSetMock.next()).thenReturn(true).thenReturn(false)

    val deviceLocation = H2DB.readLocation(sqlString)
    deviceLocation should be eq(DeviceStateDistrict(state = "Karnataka", districtCustom = "KA"))
  }


  "H2DB util: execute" should "execute the query and return results" in {
    reset(connectionMock)
    reset(preparedStatementMock)
    reset(resultSetMock)

    val sqlString = "Select * from geo_location_city_ipv4"
    when(connectionMock.prepareStatement(sqlString)).thenReturn(preparedStatementMock)
    when(preparedStatementMock.executeQuery()).thenReturn(resultSetMock)

    H2DB.execute(sqlString)
    verify(preparedStatementMock, times(1)).executeQuery()
  }

  "H2DB util: executeQuery" should "execute the query" in {
    reset(connectionMock)
    reset(preparedStatementMock)
    reset(resultSetMock)

    val sqlString = "Select * from geo_location_city_ipv4"
    when(connectionMock.prepareStatement(sqlString)).thenReturn(preparedStatementMock)

    H2DB.executeQuery(sqlString)
    verify(preparedStatementMock, times(1)).execute()
  }

}
