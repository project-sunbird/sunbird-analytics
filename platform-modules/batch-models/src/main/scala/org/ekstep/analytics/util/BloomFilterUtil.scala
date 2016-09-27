package org.ekstep.analytics.util

import breeze.util.BloomFilter
import org.ekstep.analytics.framework.Period._
import java.util.BitSet

object BloomFilterUtil {

    def getBloomFilter(period: Period): BloomFilter[String] = {
        period match {
            case DAY        => new BloomFilter[String](1000, 5, new BitSet(1000));
            case WEEK       => new BloomFilter[String](10000, 5, new BitSet(10000));
            case MONTH      => new BloomFilter[String](50000, 5, new BitSet(50000));
            case CUMULATIVE => new BloomFilter[String](100000, 5, new BitSet(100000));
        }
    }

    def serialize(bf: BloomFilter[String]): Array[Byte] = {
        bf.bits.toByteArray();
    }

    def deserialize(period: Period, bytes: Array[Byte]): BloomFilter[String] = {
        val bits = BitSet.valueOf(bytes);
        period match {
            case DAY        => new BloomFilter[String](1000, 5, bits);
            case WEEK       => new BloomFilter[String](10000, 5, bits);
            case MONTH      => new BloomFilter[String](50000, 5, bits);
            case CUMULATIVE => new BloomFilter[String](100000, 5, bits);
        }
    }
}