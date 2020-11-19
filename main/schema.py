schema = {
    "type": "object",
    "properties": {
        "city_id": {"type": "integer"},
        "driverinfo": {"type": "object",
                       "properties": {
                           "data": {"type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "aid": {"type": "integer"},
                                            "city_id": {"type": "integer"},
                                            "lat": {"type": "string"},
                                            "lng": {"type": "string"},
                                            "orders": {"type": "array",
                                                       "items": {
                                                           "type": "object",
                                                           "properties": {
                                                               "id": {"type": "integer"},
                                                               "customer_id": {"type": "integer"},
                                                               "customer_lat": {"type": "string"},
                                                               "customer_lng": {"type": "string"},
                                                               "shop_id": {"type": "integer"},
                                                               "shop_lat": {"type": "string"},
                                                               "shop_lng": {"type": "string"},
                                                               "created_at": {"type": "integer"},
                                                               "cookingtime_result": {"type": "integer"},
                                                               "city_id": {"type": "integer"}
                                                           }, "required": ["id", "customer_id", "customer_lat", "customer_lng", "shop_id",
                                                                           "shop_lat", "shop_lng", "created_at", "cookingtime_result", "city_id"]
                                                       }
                                                       }
                                        }, "required": ["aid","city_id", "lat", "lng", "orders"]
                                    }
                                    }
                       }, "required": ["data"]
                       },
        "orderinfo": {"type": "object",
                              "properties": {
                                  "data": {"type": "array",
                                           "items": {
                                               "type": "object",
                                               "properties": {
                                                   "id": {"type": "integer"},
                                                   "customer_id": {"type": "integer"},
                                                   "customer_lat": {"type": "string"},
                                                   "customer_lng": {"type": "string"},
                                                   "shop_id": {"type": "integer"},
                                                   "shop_lat": {"type": "string"},
                                                   "shop_lng": {"type": "string"},
                                                   "created_at": {"type": "integer"},
                                                   "cookingtime_result": {"type": "integer"},
                                                   "city_id": {"type": "integer"}
                                               }, "required": ["id", "customer_id", "customer_lat", "customer_lng", "shop_id",
                                                               "shop_lat", "shop_lng", "created_at", "cookingtime_result", "city_id"]
                                           }
                                           }
                              }, "required": ["data"]
                      },
        "order_id": {"type": "integer"},
        "rejection": {"type": "array",
                      "items": {
                          "type": "object",
                          "properties": {
                              "driver_id": {"type": "integer"},
                              "order_id": {"type": "integer"}
                          }, "required": ["driver_id", "order_id"]
                      }
                      }
    }, "required": ["city_id", "driverinfo", "orderinfo"]
}
