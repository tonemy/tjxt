package model;

import lombok.Data;

/**
 * @Author: 张今天
 * @Date: 2020/4/24 14:48
 */
@Data
public class Shop {
    private String id;
    private String title;
    private String imgUrl;
    private String categories;
    private String tags;
}
