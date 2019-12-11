package org.ekstep.analytics.api.util

import java.sql.{ResultSet, Timestamp}
import java.util.Date

import com.google.common.collect.Table
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.framework.util.HTTPClient
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._

class TestCacheUtil extends BaseSpec {

    val postgresDBMock = mock[PostgresDBUtil]
    val H2DBUtilMock = mock[H2DBUtil]
    val resultSetMock = mock[ResultSet]

    val cacheUtil = new CacheUtil(postgresDBMock, H2DBUtilMock)

    "Cache util " should "refresh device location cache" in {
      when(postgresDBMock.readGeoLocationCity(ArgumentMatchers.any())).thenReturn(List(GeoLocationCity(geoname_id = 29, subdivision_1_name = "Karnataka", subdivision_2_custom_name = "Karnataka")))
      when(postgresDBMock.readGeoLocationRange(ArgumentMatchers.any())).thenReturn(List(GeoLocationRange(1234, 1234, 1)))
      when(H2DBUtilMock.execute(ArgumentMatchers.any())).thenReturn(resultSetMock)
      when(resultSetMock.next()).thenReturn(true).thenReturn(true).thenReturn(false)

      cacheUtil.initDeviceLocationCache()
      verify(H2DBUtilMock, times(6)).executeQuery(ArgumentMatchers.any())

      when(postgresDBMock.readGeoLocationCity(ArgumentMatchers.any())).thenThrow(new RuntimeException("something went wrong!"))
      cacheUtil.initDeviceLocationCache()
    }


    it should "cache consumer channel" in {
      when(postgresDBMock.read(ArgumentMatchers.any())).thenReturn(List(ConsumerChannel(consumerId = "Ekstep", channel = "in.ekstep", status = 0, createdBy = "System", createdOn = new Timestamp(new Date().getTime), updatedOn = new Timestamp(new Date().getTime))))

      cacheUtil.initConsumerChannelCache()
      verify(postgresDBMock, times(1)).read(ArgumentMatchers.any())

      when(postgresDBMock.read(ArgumentMatchers.any())).thenThrow(new RuntimeException("something went wrong!"))
      cacheUtil.initConsumerChannelCache()
    }

    it should "populate consumer channel table" in {
      reset(postgresDBMock)
      when(postgresDBMock.read(ArgumentMatchers.any())).thenReturn(List(ConsumerChannel(consumerId = "Ekstep", channel = "in.ekstep", status = 0, createdBy = "System", createdOn = new Timestamp(new Date().getTime), updatedOn = new Timestamp(new Date().getTime))))
      val cacheUtilSpy = spy(cacheUtil)
      cacheUtilSpy.getConsumerChannlTable()
      verify(cacheUtilSpy, times(1)).initConsumerChannelCache()

      when(postgresDBMock.read(ArgumentMatchers.any())).thenReturn(List(ConsumerChannel(consumerId = "Ekstep", channel = "in.ekstep", status = 0, createdBy = "System", createdOn = new Timestamp(new Date().getTime), updatedOn = new Timestamp(new Date().getTime))))
      val result = cacheUtilSpy.getConsumerChannlTable()
      result.isInstanceOf[Table[String, String, Integer]] should be (true)
    }

    it should "validate cache" in {
      val cacheUtilSpy = spy(cacheUtil)
      cacheUtilSpy.validateCache()
      verify(cacheUtilSpy, times(1)).initCache()
    }
}
