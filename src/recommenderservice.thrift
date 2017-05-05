/**
 * Structs can also be exceptions, if they are nasty.
 */
exception InvalidOperation {
  1: i32 whatOp,
  2: string why
}

struct Request {
    1: optional string ad_id;
    2: optional string user_id;
    3: optional string cityName;
    4: optional string category
}


service Recommender {

   void ping(),

   list<string> fetchRec(1:Request req) throws (1:InvalidOperation ouch)

}