# 휴가 모드 자동 싱크 (Vacation Redash Sync)

휴가 중 Mac이 꺼져 있어도 **매일 평일 오전 11시**에 네이버 SA 성과 데이터를 자동으로 구글 시트에 업데이트해주는 GitHub Actions 자동화입니다.

- VPN 불필요 — BigQuery 직접 쿼리
- OTP/이메일 응답 불필요 — 완전 자동
- 완료 시 이메일 알림 발송

---

## 동작 방식

```
vacation.json에 휴가 기간 설정
       ↓
GitHub Actions 매일 11:00 KST 실행
       ↓
BigQuery 쿼리 (파워링크 / 쇼검광)
       ↓
Google Sheets 업데이트 (내부용 + 대행사용)
       ↓
완료 알림 이메일 발송
```

---

## 세팅 방법

### 1. 이 레포 Fork

우측 상단 **Fork** 버튼 클릭

### 2. GitHub Secrets 등록

Fork한 레포의 **Settings → Secrets and variables → Actions**에서 아래 7개 등록:

| Secret 이름 | 내용 |
|------------|------|
| `BQ_TOKEN` | BigQuery OAuth 토큰 JSON (`bq_token.json` 파일 내용 전체) |
| `GOOGLE_CREDENTIALS` | Google API OAuth credentials JSON |
| `GOOGLE_TOKEN` | Google Sheets OAuth 토큰 JSON (`token.json` 파일 내용 전체) |
| `GMAIL_USER` | 알림 발송용 Gmail 주소 (예: yourname@gmail.com) |
| `GMAIL_APP_PASSWORD` | Gmail 앱 비밀번호 ([발급 방법](https://support.google.com/accounts/answer/185833)) |
| `WORK_EMAIL` | 완료 알림 받을 이메일 주소 |

> **BQ_TOKEN 발급 방법**
> ```bash
> cd ~/redash_sync
> python3 -c "import json; print(open('bq_token.json').read())"
> ```
> 출력된 JSON 전체를 Secret 값으로 등록

### 3. sync.py 상단 설정값 수정

```python
# ── ✏️ 여기만 수정하세요 ───────────────────────────
SHEET_ID        = '내부용_구글시트_ID'
SHEET_ID_AGENCY = '대행사용_구글시트_ID'
BQ_PROJECT      = 'mrtdata'
```

구글 시트 ID는 URL에서 확인:
`https://docs.google.com/spreadsheets/d/여기가_ID/edit`

### 4. vacation.json 설정

휴가 기간을 설정하고 push하면 해당 기간에만 자동 실행됩니다.

```json
[{"start": "2026-05-01", "end": "2026-05-05"}]
```

- 여러 기간 등록 가능: `[{"start": "...", "end": "..."}, {"start": "...", "end": "..."}]`
- 휴가 아닌 날은 실행 안 됨 (평일 스케줄은 있지만 조건 미충족으로 스킵)

---

## 수동 테스트

GitHub Actions 탭 → **Vacation Redash Sync** → **Run workflow** 클릭

(vacation.json에 오늘 날짜가 포함되어 있어야 sync 단계까지 실행됨)

---

## 구글 시트 구조

| 탭 이름 | 시트 | 내용 |
|--------|------|------|
| 파워링크_내부 | 내부용 | 키워드별 날짜별 + CM 수치 |
| 파워링크 | 내부용 + 대행사용 | CM 수치 제외 |
| 파워링크_주차별 CM 성과 | 대행사용 | 주차별 집계 + 신호등 |
| 쇼검광_내부 | 내부용 | 상품별 날짜별 + CM 수치 |
| 쇼검광 | 내부용 + 대행사용 | CM 수치 제외 |
| 쇼검광_주차별 CM 성과 | 대행사용 | 주차별 집계 + 신호등 |

## 신호등 기준 (CM ROAS)

| 신호 | 기준 | 의미 |
|------|------|------|
| 🟢 | 200% 이상 | 성과 매우 우수 + 적극 상향 조정 |
| 🔵 | 100~200% | 성과 준수 + 상향 조정 |
| 🟡 | 50~100% | 성과 미달 |
| 🔴 | 50% 미만 | 즉각 하향 조정 필요 |
