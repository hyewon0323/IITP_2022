# IITP_2022
IITP 2차년도 demo

2TB 대용량 개인정보 파일 가명/익명화 처리 프로그램 개발

```
- TopBottom: 3시그마 이상 값을 mean으로 대체
- Masking: 특정 부분을 *로 대체
- Delete: 열 삭제
- Part delete: 특정 부분을 삭제
- Round: 기준 자릿수에서 rounding
- Encryption: 열 전체 암호화
- Aggregation: 총계처리(mean으로 대체)
- Random : 열 전체 randomize 
```

가명화하고자 하는 열 번호와 조건이 적힌 Benchmark.json 이 필요함
```
{
3: "Masking, 5",
5: "Encryption",
12: "Round, 3"
}
```

실행방법
```bash
mvn clean install
hadoop jar target/demo-1.0.jar example.App {input hdfs path} {output hdfs path} columns.json 
```
