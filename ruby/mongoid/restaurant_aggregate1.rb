Restaurant.collection.aggregate(
    [
      {'$match':
         {'_id': restaurant.id}},
      {'$lookup': {
        from: Hotel.collection_name.to_s,
        "let": {"restaurantId": "$_id"},
        "pipeline": [
          {"$unwind": {path: "$linked_to_restaurants", preserveNullAndEmptyArrays: true}},
          {"$match": {"$expr": {"$eq": %w{$linked_to_restaurants.id $$restaurantId}}}}],
        as: "hotels"}},
      {"$unwind": {path: "hotels", preserveNullAndEmptyArrays: true}},
      {"$unwind": {path: "hotels.linked_to_cities", preserveNullAndEmptyArrays: true}},
      {'$lookup': {
        from: City.collection_name.to_s,
        "let": {"hotelId": "hotels.linked_to_hotels.id"},
        "pipeline": [
          {"$match": {
            "$expr": {
              "$and": [
                {"$eq": %w{$_id $$hotelId}},
                {"$eq": ["$city_name", city_name]}
              ]}}}],
        as: "cities"}},
      {"$unwind": {path: "cities"}},
      {'$project': {
        _id: 0,
        hotelId: "$hotels._id",
        hotelOfType: "$hotels._type",
        hotelName: "$hotels.hotel_name",
        restaurantId: "$_id",
        restaurantOfType: "$_type",
        restaurantName: "$restaurant_name",
        cityId: "$cities._id",
        cityType: "$cities._type",
        cityName: "$cities.city_name",
        cityLastModified: "$cities.last_modified",
      }},
      {"$sort": {hotelId: 1, cityId: 1, }}
    ])