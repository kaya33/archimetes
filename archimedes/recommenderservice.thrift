
exception InvalidOperation {
  1: i32 whatOp,
  2: string why
}

enum responseType {
	OK = 1,
	ERROR
}

struct ItemRequest {
    1: required string item_id;
    2: optional string cityName;
    3: optional string category
}

struct UserRequest {
    1: required string user_id;
    2: optional string cityName;
    3: optional string category
}

struct RecResponse {
	1: required responseType status;
	2: required string errStr;
	3: required list<map<string,string>> data
}

service Recommender {

   string ping(),

   RecResponse fetchRecByItem(1:ItemRequest req);

   RecResponse fetchRecByUser(1:UserRequest req)
}