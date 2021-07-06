package com.xianghu.hudi.data.generator;

import com.alibaba.fastjson.JSON;
import com.xianghu.hudi.data.model.StockTick;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class HoodieDataCreater {

  public static List<String> create(int batch) {
    List<String> result = new ArrayList<>(batch);
    for (int i = 0; i < batch; i++) {
      StockTick stockTick = new StockTick();
      Calendar calendar = Calendar.getInstance();
      Random random = new Random();
      String key = i % 5 + "";

      stockTick.setUuid(UUID.randomUUID().toString());
      stockTick.setTs(calendar.getTimeInMillis());
      stockTick.setSymbol(UUID.randomUUID().toString().substring(0,8));
      stockTick.setYear(calendar.get(Calendar.YEAR));
      stockTick.setMonth(calendar.get(Calendar.MONTH));
      stockTick.setDay(String.valueOf(calendar.get(Calendar.MONTH)));
      stockTick.setHigh(5.33 + random.nextDouble());
      stockTick.setLow(2.33 - random.nextDouble());
      stockTick.setKey(key);
      stockTick.setClose(i / 2 == 0 ? stockTick.getHigh() : stockTick.getLow());
      stockTick.setOpen(i / 2 == 0 ? stockTick.getLow() : stockTick.getHigh());

      result.add(JSON.toJSONString(stockTick));
    }
    return result;
  }
}
