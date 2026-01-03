# WebSocketServer Copilot Instructions

## ⚠️ CRITICAL
**절대 `doc/Define.py` 수정 금지** (운영 DB/키/장치 목록 포함). 읽기만 허용.

## Big Picture
- [main.py](../main.py): WebSocket 서버(포트 49693) 엔트리포인트. `safe_recv()`(3초 타임아웃)→인증→서비스 라우팅, `safe_send()`로만 송신(전역 락으로 직렬화).
- [module/Auth.py](../module/Auth.py): 로그인/로그아웃/핑/검증(`varifiy` 철자 유지) 로직. 로그인 실패 시 1초 지연으로 브루트포스 완화.
- [core/SqlManager.py](../core/SqlManager.py): `aiomysql` 풀 기반 비동기 DB. 시작 시 `Blog.user_login`(MEMORY) 테이블을 DROP/CREATE 하므로 재시작하면 로그인 세션이 초기화됨.
- [module/ServiceWOL.py](../module/ServiceWOL.py), [module/StockTickerManager.py](../module/StockTickerManager.py): 서비스별 비즈니스 로직(반드시 `(result:int, msg:str, data:dict)` 반환).
- 로깅: [core/Util.py](../core/Util.py)의 `InsertLog(name, type, msg)` 사용(백그라운드 쓰레드로 Log DB 적재).

## Wire Protocol (클라이언트↔서버)
- 요청 JSON: `{ "service": "auth|wol|stm", "work": "...", "data": { ... } }`
- 응답 JSON: `{ "service": "...", "result": int, "msg": str, "data": {} }`
- 첫 메시지는 반드시 로그인(그 외면 즉시 종료될 수 있음).
- 매 요청마다 `auth/varifiy`로 `login_hash` 검증 후 서비스 처리(인증 절차 변경 금지).
- `varifiy`는 성공 시 응답을 생략하는 동작이 있음(클라이언트는 매 요청마다 별도 verify 응답을 기대하지 말 것).
- `safe_recv()` 타임아웃/파싱 실패 시 `{service:"late"...}`를 반환하며 메인 루프는 이를 무시함.
- 연결 유지는 `auth/ping`으로 `client_info["ping"]`을 갱신해야 하며, 루프는 `Define.WS_LATE_PING_SEC` 기준으로 종료될 수 있음.

## SQL 규칙 (인젝션 방어)
- 값(value)은 무조건 파라미터 바인딩:
  - `sql_manager.Get("... WHERE x=%s", (x,))`
  - `sql_manager.Set([("INSERT ... VALUES (%s,%s)", (a,b)), "DELETE ..."])`
- `Set()`은 트랜잭션으로 일괄 커밋되며 반환코드: `0` 성공, `1062` 중복키, `-1` 실패.
- 식별자(identifier: DB/스키마/테이블/컬럼명)는 바인딩 불가 → **화이트리스트/정규식 검증 후 백틱으로 조합**.
- 예시 패턴: [module/StockTickerManager.py](../module/StockTickerManager.py) `GetCandleData()`는 `target_code`를 정규식 검증 후 `` `{schema}`.`{table}` `` 형태로만 조합.
- 파라미터 쿼리에서 `DATE_FORMAT` 같은 `%` 포맷은 `%%`로 이스케이프 필요(예: `'%%Y%%m%%d%%H%%i%%s'`).

## Dev Workflow
- 실행: `python main.py` (시작 시 `Blog.user_login` MEMORY 테이블 초기화)
- 빠른 문법 체크: `python -m py_compile $(find . -type f -name '*.py' -not -path './temp/*' -not -path './**/__pycache__/*')`
- 의존성(예): `websockets`, `aiomysql`, `pymysql` (환경에 없으면 에디터에서 import 경고가 뜰 수 있음)

## 새 서비스 추가
- [main.py](../main.py)에서 라우팅 추가: 가능하면 `_dispatch_work(req_work, mapping, ...)` 패턴으로 work→함수 매핑.
- 권한은 서비스 함수 초기에 `client_info["user_level"]`로 가드(기존 모듈들이 이 방식 사용).
- [module/](../module/)에 서비스 함수 추가: 항상 `(result, msg, data)` 반환하고, 핸들러에서 `{service,result,msg,data}`로 감싼 뒤 `safe_send()`.
