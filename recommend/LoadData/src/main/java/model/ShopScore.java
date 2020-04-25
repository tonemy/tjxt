package model;

import lombok.Data;

/**
 * @Author: 张今天
 * @Date: 2020/4/25 11:53
 */

@Data
public class ShopScore {
    private Integer id;
    private Integer shopId;
    private Double score;
    private Integer timestamp;
}
