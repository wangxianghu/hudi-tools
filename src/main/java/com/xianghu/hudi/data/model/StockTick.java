package com.xianghu.hudi.data.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockTick implements Serializable {
  private String uuid;
  private Long ts;
  private String symbol;
  private Integer year;
  private Integer month;
  private Double high;
  private Double low;
  private String key;
  private Double close;
  private Double open;
  private String day;

}
