package com.atguigu.spark.streamingproject.bean

import java.text.SimpleDateFormat
import java.util.Date
;

case class AdsInfo(ts: Long,
        area: String,
        city: String,
        userId: String,
        adsId: String,
        var dayString: String = null, // yyyy-MM-dd
        var hmString: String = null) { // hh:mm

        val date = new Date(ts)
        dayString = new SimpleDateFormat("yyyy-MM-dd").format(date)
        hmString = new SimpleDateFormat("HH:mm").format(date)
}
