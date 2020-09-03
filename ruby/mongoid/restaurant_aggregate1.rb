Restaurant.collection.aggregate(
    [
      {'$match':
         {'_id': restaurant.id,
          "linked_to_hotels.id": {"$in": [hotel.id]}}},
      {'$lookup': {
        from: Location.collection_name.to_s,
        "let": {"restaurantId": "$_id"},
        "pipeline": [
          {"$unwind": {path: "$linked_to_restaurants", preserveNullAndEmptyArrays: true}},
          {"$match": {"$expr": {"$eq": %w{$linked_to_restaurants.id $$restaurantId}}}}],
        as: "locations"}},
      {"$unwind": {path: "$locations", preserveNullAndEmptyArrays: true}},
      {"$unwind": {path: "$locations.linked_to_hotels", preserveNullAndEmptyArrays: true}},
      {'$lookup': {
        from: BaseHotel.collection_name.to_s,
        "let": {"hotelId": "$locations.linked_to_hotels.id"},
        "pipeline": [
          {"$match": {
            "$expr": {
              "$and": [
                {"$eq": %w{$_id $$hotelId}},
                {"$eq": ["$creator.id", user_id]}
              ]}}}],
        as: "hotels"}},
      {"$unwind": {path: "$hotels"}},
      {'$project': {
        _id: 0,
        locationId: "$locations._id",
        locationOfType: "$locations._type",
        locationName: "$locations.location_name",
        locationLinkedToHotelId: "$locations.linked_to_hotels.id",
        restaurantId: "$_id",
        restaurantOfType: "$_type",
        restaurantName: "$restaurant_name",
        hotelId: "$hotels._id",
        hotelType: "$hotels._type",
        hotelName: "$hotels.hotel_name",
        hotelLastModified: "$hotels.last_modified",
      }},
      {"$sort": {hotelId: 1, locationId: 1, }}
    ])