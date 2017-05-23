namespace php Recommender.Thrift


enum responseType {
	OK = 1,
	ERROR=0
}

enum ErrorCode {
    SYSTEM_ERROR = 0
    USER_NOT_FOUND = 1
}

// 系统错误
exception SystemException {
  1: required ErrorCode code,
  2: required string name,
  3: optional string message,
}

// 业务逻辑产生的错误
exception CodeException {
  1: required ErrorCode code,
  2: required string name,
  3: optional string message,
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
    3: optional string first_category;
    4: optional string second_category;
    5: optional i32 size
}

struct MultRequest {
    1: required string user_id;
    3: optional string city_name;
    4: optional string first_category;
    5: optional string second_category;
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

   RecResponse fetchRecByItem(1:ItemRequest req) throws (1: SystemException sys_exc,
                                                             2: CodeException code_exc);

   RecResponse fetchRecByUser(1:UserRequest req) throws (1: SystemException sys_exc,
                                                             2: CodeException code_exc);

   RecResponse fetchRecByMult(1:MultRequest req) throws (1: SystemException sys_exc,
                                                             2: CodeException code_exc)
}
