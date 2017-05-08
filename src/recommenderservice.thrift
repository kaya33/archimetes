/**
 * Structs can also be exceptions, if they are nasty.
 */
exception InvalidOperation {
  1: i32 whatOp,
  2: string why
}

struct UserRequest {
    1: required string user_id;
    2: optional string cityName;
    3: optional string category
}

struct ItemRequest {
    1: required string item_id;
    2: optional string cityName;
    3: optional string category
}

struct RecResult {
    1: required i32 errNum;
    2: required string errStr;
    3: required list<map<string,string>> data
}

service Recommender {

   void ping(),

   RecResult fetchRecByItem(1:ItemRequest req) throws (1:InvalidOperation ouch);

   RecResult fetchRecByUser(1:UserRequest req) throws (1:InvalidOperation ouch)

}