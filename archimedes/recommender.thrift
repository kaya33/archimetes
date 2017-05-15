
exception InvalidOperation {
  1: i32 what_op,
  2: string why
}

enum responseType {
	OK = 1,
	ERROR=0
}

struct ItemRequest {
    1: required string ad_id;
    2: optional string city_name;
    3: optional string category_name;
    4: optional i32 size
}

struct UserRequest {
    1: required string user_id;
    2: optional string city_name;
    3: optional string first_cat;
    4: optional string second_cat;
    5: optional i32 size
}

struct MultRequest {
    1: required string user_id;
    2: required string ad_id;
    3: optional string city_name;
    4: optional string first_cat;
    5: optional string second_cat;
    6: optional i32 ssize
}

struct OneRecResult {
    1: required string rec_id;
    2: optional string rec_name
}


struct RecResponse {
	1: required responseType status;
	2: required string err_str;
	3: required list<OneRecResult> data
}

service Recommender {

   string ping(),

   RecResponse fetchRecByItem(1:ItemRequest req);

   RecResponse fetchRecByUser(1:UserRequest req);

   RecResponse fetchRecByMult(1:MultRequest req)
}